#pragma once

#include <span>
#include <vector>

#include "TargetSelection.h"
#include "UpdateChannelAllocator.h"
#include "client/mgmtd/ICommonMgmtdClient.h"
#include "common/net/Client.h"
#include "common/utils/Address.h"
#include "common/utils/Coroutine.h"
#include "common/utils/Result.h"
#include "common/utils/Semaphore.h"
#include "fbs/mgmtd/RoutingInfo.h"
#include "fbs/storage/Common.h"

namespace hf3fs::storage::client {

/*
  3FS StorageClient 层概览（客户端侧存储访问入口）：
  - 作用：负责把用户的读写/维护类操作封装为统一的请求，依据路由信息选择目标存储节点，做并发与重试控制，
          并通过网络消息通道与 StorageService 交互。
  - 关键组成：
    * RoutingTarget：面向复制链（Chain）的当前可用目标与通道信息，支持链版本与路由版本一致性检查。
    * ReadIO/WriteIO 及维护操作（Query/Remove/Truncate）：封装一次操作的参数、结果与上下文。
    * Options/Config：读写选项（校验、目标选择、重试）与客户端全局配置（并发流控、网络客户端）。
    * UpdateChannelAllocator：为需要顺序性的更新类操作分配有序通道（channel），保证同一链上更新的先后关系。
  - 读链路：
    用户构造 ReadIO → StorageClientImpl 选择目标（可负载均衡）并按节点分组 → 并发批量发送 → 可选内联回传数据 →
    可选客户端校验 → 记录指标与返回结果。
  - 写链路：
    用户构造 WriteIO → 选择链头目标（或指定策略）→ 为每个 IO 分配更新通道 → 按节点顺序发送（保证有序）→
    服务端校验并落盘 → 成功后释放通道 → 记录指标与返回结果。
  - 维护链路（查询/删除/截断）：流程与写类似，通常选择链头，必要时也使用更新通道以保证操作序。
*/

/*
  动态路由目标信息：
  - 表示一次操作要访问的复制链上的某个存储目标以及与之关联的有序通道。
  - `chainId/chainVer`：复制链及其版本，用于确保与最新路由一致。
  - `routingInfoVer`：路由表版本；客户端在发送前会比对，防止基于过期路由进行访问。
  - `targetInfo`：目标存储节点的轻量信息（目标/节点标识）。
  - `channel`：更新类操作的通道号，用来在服务端进行跨请求的顺序约束；读操作一般不需要。
*/
class RoutingTarget {
 public:
  RoutingTarget(ChainId chainId)
      : chainId(chainId) {}

  ~RoutingTarget() {
    XLOGF_IF(DFATAL,
             channel.id != ChannelId{0},
             "Leaked update channel, routing target: {}, stack trace: {}",
             *this,
             folly::symbolizer::getStackTraceStr());
  }

  const hf3fs::storage::VersionedChainId getVersionedChainId() const { return {chainId, chainVer}; };

 public:
  ChainId chainId;
  ChainVer chainVer;
  flat::RoutingInfoVersion routingInfoVer;
  SlimTargetInfo targetInfo;
  UpdateChannel channel;
};

/*
  IOBuffer：注册到 RDMA 的用户缓冲区视图
  - 通过 `registerIOBuffer` 生成，内部持有 RDMA 缓冲描述。
  - 支持计算子区间 `subrange`，为零拷贝发送/接收提供基础。
*/
class IOBuffer : public folly::MoveOnly {
 public:
  uint8_t *data() const { return const_cast<uint8_t *>(rdmabuf.ptr()); }

  size_t size() const { return rdmabuf.size(); }

  bool contains(const uint8_t *data, uint32_t len) const { return rdmabuf.contains(data, len); }

  net::RDMABuf subrange(size_t offset, size_t length) const { return rdmabuf.subrange(offset, length); }

  // IOBuffer 封装了客户端已注册的 RDMA 内存：
  // - `rdmabuf` 为已注册的本地缓冲；`data()/size()` 提供读写窗口
  // - `contains(data,len)` 用于校验用户数据区间是否落在注册范围，避免越界
  // - `subrange()` 用于按 IO 切分窗口，便于批量 RDMA 组织
  IOBuffer(hf3fs::net::RDMABuf rdmabuf)
      : rdmabuf(rdmabuf) {}

 private:
  const hf3fs::net::RDMABuf rdmabuf;

  friend class IOBase;
  friend class StorageClient;
  friend class StorageClientImpl;
  friend class StorageClientInMem;
};

/*
  IOBase：一次读/写操作的通用参数与结果容器
  - `chunkId/offset/length/chunkSize`：块标识与读写范围；写操作会校验范围合法性。
  - `data/buffer`：用户数据指针与已注册的 RDMA 缓冲区；需保证在同批请求中不重叠（可配置关闭）。
  - `routingTarget`：目标链与通道信息，由实现层结合路由选择。
  - `result`：服务端返回的长度/状态与校验信息；`status()`/`statusCode()` 提供统一错误码访问。
*/
class IOBase : public folly::MoveOnly {
 private:
  IOBase(ChainId chainId,
         const ChunkId &chunkId,
         uint32_t offset,
         uint32_t length,
         uint32_t chunkSize,
         uint8_t *data,
         IOBuffer *buffer,
         void *userCtx)
      : routingTarget(chainId),
        chunkId(chunkId),
        offset(offset),
        length(length),
        chunkSize(chunkSize),
        data(data),
        buffer(buffer),
        userCtx(userCtx) {}

 public:
  Status status() const { return result.lengthInfo ? Status(StatusCode::kOK) : result.lengthInfo.error(); }
  status_code_t statusCode() const { return hf3fs::getStatusCode(result.lengthInfo); }
  uint32_t resultLen() const { return result.lengthInfo ? *result.lengthInfo : 0; }
  uint32_t dataLen() const { return length; }
  uint8_t *dataEnd() const { return data + length; }
  ChunkIdRange chunkRange() const { return {chunkId, chunkId, 1}; }
  uint32_t numProcessedChunks() const { return bool(result.lengthInfo); }
  void resetResult() { result = IOResult{}; }

 public:
  RoutingTarget routingTarget;
  ChunkId chunkId;
  const uint32_t offset;
  const uint32_t length;
  const uint32_t chunkSize;
  uint8_t *const data;
  IOBuffer *const buffer;
  void *const userCtx;
  IOResult result;

  friend class ReadIO;
  friend class WriteIO;
};

/*
  ReadIO：一次读操作
  - `splittedIOs`：当单次读取超过阈值时，客户端会拆分为多个子读请求并在合并结果时保持一致性。
*/
class ReadIO : public IOBase {
 private:
  ReadIO(ChainId chainId,
         const ChunkId &chunkId,
         uint32_t offset,
         uint32_t length,
         uint8_t *data,
         IOBuffer *buffer,
         void *userCtx)
      : IOBase(chainId, chunkId, offset, length, 0 /*chunkSize*/, data, buffer, userCtx) {}

  friend class StorageClient;
  friend class StorageClientImpl;
  friend class StorageClientInMem;

 public:
  // ReadIO：客户端读操作描述
  // - `key` 在 RPC 序列化结构中携带链与 chunk 信息；`rdmabuf` 为客户端远端缓冲，由服务端 RDMA WRITE 回填
  // - 服务端在 AIO 完成后，将 `state.localbuf` 按窗口写入 `rdmabuf` 或选择 inline 返回
  RequestId requestId;
  std::vector<ReadIO> splittedIOs;
};

/*
  WriteIO：一次写操作
  - `requestId`：客户端侧请求编号，用于日志与去重校验。
  - `checksum`：可选端到端校验（由客户端计算并透传到服务端核对）。
*/
class WriteIO : public IOBase {
 private:
  WriteIO(RequestId requestId,
          ChainId chainId,
          const ChunkId &chunkId,
          uint32_t offset,
          uint32_t length,
          uint32_t chunkSize,
          uint8_t *data,
          IOBuffer *buffer,
          void *userCtx)
      : IOBase(chainId, chunkId, offset, length, chunkSize, data, buffer, userCtx),
        requestId(requestId) {}

 public:
  // WriteIO：客户端写操作描述
  // - `rdmabuf` 为客户端远端缓冲，服务端通过 RDMA READ 拉取数据；也可根据阈值选择 inline 发送
  // - `checksum` 为客户端本地数据校验，服务端会与自身计算结果比对，保障数据一致性
  const ChecksumInfo &localChecksum() const { return checksum; }

 private:
  friend class StorageClient;
  friend class StorageClientImpl;
  friend class StorageClientInMem;

 public:
  const RequestId requestId;
  ChecksumInfo checksum;
};

/*
  读写选项与调试注入：
  - DebugOptions：支持绕过磁盘 IO/网络发送、故障注入（客户端/服务端）等，仅在开发或测试模式使用。
  - RetryOptions：覆盖客户端重试策略的局部参数（初始/最大等待、总重试时长、是否重试永久错误）。
  - ReadOptions：是否启用校验、是否允许读取未提交数据（如链复制未完全成功时）。
  - WriteOptions：是否启用端到端校验；目标选择仅用于测试。
*/
class DebugOptions : public hf3fs::ConfigBase<DebugOptions> {
  CONFIG_HOT_UPDATED_ITEM(bypass_disk_io, false);
  CONFIG_HOT_UPDATED_ITEM(bypass_rdma_xmit, false);
  CONFIG_HOT_UPDATED_ITEM(inject_random_server_error, false);
  CONFIG_HOT_UPDATED_ITEM(inject_random_client_error, false);
  CONFIG_HOT_UPDATED_ITEM(max_num_of_injection_points, 100);

 public:
  DebugFlags toDebugFlags() const {
#ifndef NDEBUG
    return DebugFlags{
        .injectRandomServerError = inject_random_server_error(),
        .injectRandomClientError = inject_random_client_error(),
        .numOfInjectPtsBeforeFail = (uint16_t)folly::Random::rand32(1, max_num_of_injection_points() + 1)};
#else
    return DebugFlags{};
#endif
  }
};

class RetryOptions : public hf3fs::ConfigBase<RetryOptions> {
  CONFIG_HOT_UPDATED_ITEM(init_wait_time, Duration::zero());  // if set to zero, use the value from client config
  CONFIG_HOT_UPDATED_ITEM(max_wait_time, Duration::zero());   // if set to zero, use the value from client config
  CONFIG_HOT_UPDATED_ITEM(max_retry_time, Duration::zero());  // if set to zero, use the value from client config
  CONFIG_HOT_UPDATED_ITEM(retry_permanent_error, false);
};

class ReadOptions : public hf3fs::ConfigBase<ReadOptions> {
  CONFIG_OBJ(debug, DebugOptions);
  CONFIG_OBJ(retry, RetryOptions);
  CONFIG_OBJ(targetSelection, TargetSelectionOptions);
  CONFIG_HOT_UPDATED_ITEM(enableChecksum, false);
  CONFIG_HOT_UPDATED_ITEM(allowReadUncommitted, false);

 public:
  bool verifyChecksum() const {
#ifndef NDEBUG
    bool enabled = true;
#else
    bool enabled = enableChecksum();
#endif

    return enabled && !debug().bypass_disk_io() && !debug().bypass_rdma_xmit();
  }
};

class WriteOptions : public hf3fs::ConfigBase<WriteOptions> {
  CONFIG_OBJ(debug, DebugOptions);
  CONFIG_OBJ(retry, RetryOptions);
  CONFIG_OBJ(targetSelection, TargetSelectionOptions);  // for test only
  CONFIG_HOT_UPDATED_ITEM(enableChecksum, true);

 public:
  bool verifyChecksum() const {
#ifndef NDEBUG
    bool enabled = true;
#else
    bool enabled = enableChecksum();
#endif

    return enabled && !debug().bypass_disk_io() && !debug().bypass_rdma_xmit();
  }
};

class IoOptions : public ConfigBase<IoOptions> {
  CONFIG_OBJ(read, ReadOptions);
  CONFIG_OBJ(write, WriteOptions);
};

/*
  QueryLastChunkOp：查询区间内按字典序最大 ChunkId 的块，同时统计区间内块数与总长度。
*/
class QueryLastChunkOp : public folly::MoveOnly {
 private:
  QueryLastChunkOp(ChainId chainId, ChunkIdRange range, void *userCtx)
      : requestId(0),
        routingTarget(chainId),
        range(range),
        userCtx(userCtx) {}

 public:
  Status status() const { return result.statusCode ? Status(StatusCode::kOK) : result.statusCode.error(); }
  status_code_t statusCode() const { return hf3fs::getStatusCode(result.statusCode); }
  uint32_t resultLen() const { return 0; }
  uint32_t dataLen() const { return 0; }
  ChunkIdRange chunkRange() const { return range; }
  uint32_t numProcessedChunks() const { return result.statusCode ? result.totalNumChunks : 0; }
  void resetResult() { result = QueryLastChunkResult{}; }

 public:
  RequestId requestId;
  RoutingTarget routingTarget;
  const ChunkIdRange range;
  void *const userCtx;
  QueryLastChunkResult result;

 public:
  friend class StorageClient;
  friend class StorageClientImpl;
  friend class StorageClientInMem;
};

/*
  RemoveChunksOp：删除给定区间的块；由于自动重试，统计的删除数可能小于实际执行的删除数。
*/
class RemoveChunksOp : public folly::MoveOnly {
 private:
  RemoveChunksOp(RequestId requestId, ChainId chainId, ChunkIdRange range, void *userCtx)
      : requestId(requestId),
        routingTarget(chainId),
        range(range),
        userCtx(userCtx) {}

 public:
  Status status() const { return result.statusCode ? Status(StatusCode::kOK) : result.statusCode.error(); }
  status_code_t statusCode() const { return hf3fs::getStatusCode(result.statusCode); }
  uint32_t resultLen() const { return 0; }
  uint32_t dataLen() const { return 0; }
  ChunkIdRange chunkRange() const { return range; }
  uint32_t numProcessedChunks() const { return result.statusCode ? result.numChunksRemoved : 0; }
  void resetResult() { result = RemoveChunksResult{}; }

 public:
  const RequestId requestId;
  RoutingTarget routingTarget;
  const ChunkIdRange range;
  void *const userCtx;
  RemoveChunksResult result;

  friend class StorageClient;
  friend class StorageClientImpl;
  friend class StorageClientInMem;
};

/*
  TruncateChunkOp：将块截断/扩展到指定长度；不存在则按 `chunkSize` 创建。
  - `onlyExtendChunk=true` 时只扩展，不缩短；结果中的 `lengthInfo` 返回最终长度以便端侧核对。
*/
class TruncateChunkOp : public folly::MoveOnly {
 private:
  TruncateChunkOp(RequestId requestId,
                  ChainId chainId,
                  const ChunkId &chunkId,
                  uint32_t chunkLen,
                  uint32_t chunkSize,
                  bool onlyExtendChunk,
                  void *userCtx)
      : requestId(requestId),
        routingTarget(chainId),
        chunkId(chunkId),
        chunkLen(chunkLen),
        chunkSize(chunkSize),
        onlyExtendChunk(onlyExtendChunk),
        userCtx(userCtx) {}

 public:
  Status status() const { return result.lengthInfo ? Status(StatusCode::kOK) : result.lengthInfo.error(); }
  status_code_t statusCode() const { return hf3fs::getStatusCode(result.lengthInfo); }
  uint32_t resultLen() const { return 0; }
  uint32_t dataLen() const { return 0; }
  ChunkIdRange chunkRange() const { return {chunkId, chunkId, 1}; }
  uint32_t numProcessedChunks() const { return bool(result.lengthInfo); }
  void resetResult() { result = IOResult{}; }

 public:
  const RequestId requestId;
  RoutingTarget routingTarget;
  const ChunkId chunkId;
  const uint32_t chunkLen;
  const uint32_t chunkSize;
  bool onlyExtendChunk;
  void *const userCtx;
  IOResult result;  // result.lengthInfo == chunkLen if the op succeeds

  friend class StorageClient;
  friend class StorageClientImpl;
  friend class StorageClientInMem;
};

/*
  StorageClient：3FS 客户端存储入口抽象
  - ImplementationType：具体实现（RPC 与 InMem）。
  - MethodType：统计与调度使用的操作类型枚举。
  - RetryConfig：全局重试策略（初始/最大等待、总重试时长、失败阈值触发目标切换）。
  - OperationConcurrency/HotLoadOperationConcurrency：并发与批量大小控制；HotLoad 版本支持动态热更新。
  - TrafficControlConfig：各操作类型的并发与批量限制；`max_concurrent_updates()` 估算更新类操作需要的通道容量。
  - Config：网络客户端与更新客户端配置、重试策略、流控、校验类型、缓冲区重叠检查、内联阈值等。
  - 接口：创建 IO/Op、批量与单次读写、查询/删除/截断、目标管理与空间查询、缓冲注册等。
*/
class StorageClient : public folly::MoveOnly {
 public:
  enum class ImplementationType {
    RPC,
    InMem,
  };

  enum class MethodType {
    batchRead = 1,
    batchWrite,
    read,
    write,
    queryLastChunk,
    removeChunks,
    truncateChunks,
    querySpaceInfo,
    createTarget,
    offlineTarget,
    removeTarget,
    queryChunk,
    getAllChunkMetadata,
  };

  class RetryConfig : public hf3fs::ConfigBase<RetryConfig> {
   public:
    CONFIG_HOT_UPDATED_ITEM(init_wait_time, 10_s);
    CONFIG_HOT_UPDATED_ITEM(max_wait_time, 30_s);
    CONFIG_HOT_UPDATED_ITEM(max_retry_time, 60_s);
    CONFIG_HOT_UPDATED_ITEM(max_failures_before_failover,
                            1U);  // the max number of failed retries before switching to alternative targets

   public:
    RetryOptions mergeWith(RetryOptions options) const {
      if (options.init_wait_time() == Duration::zero()) options.set_init_wait_time(this->init_wait_time());
      if (options.max_wait_time() == Duration::zero()) options.set_max_wait_time(this->max_wait_time());
      if (options.max_retry_time() == Duration::zero()) options.set_max_retry_time(this->max_retry_time());
      return options;
    }
  };

  class OperationConcurrency : public hf3fs::ConfigBase<OperationConcurrency> {
    CONFIG_ITEM(max_batch_size, 128U);
    CONFIG_ITEM(max_batch_bytes, 4_MB);
    CONFIG_ITEM(max_concurrent_requests, 32U);
    CONFIG_ITEM(max_concurrent_requests_per_server, 8U);
    CONFIG_HOT_UPDATED_ITEM(random_shuffle_requests, true);
    CONFIG_HOT_UPDATED_ITEM(process_batches_in_parallel, true);
  };

  class HotLoadOperationConcurrency : public hf3fs::ConfigBase<HotLoadOperationConcurrency> {
    CONFIG_HOT_UPDATED_ITEM(max_batch_size, 128U);
    CONFIG_HOT_UPDATED_ITEM(max_batch_bytes, 4_MB);
    CONFIG_HOT_UPDATED_ITEM(max_concurrent_requests, 32U);
    CONFIG_HOT_UPDATED_ITEM(max_concurrent_requests_per_server, 8U);
    CONFIG_HOT_UPDATED_ITEM(random_shuffle_requests, true);
    CONFIG_HOT_UPDATED_ITEM(process_batches_in_parallel, true);
  };

  class TrafficControlConfig : public hf3fs::ConfigBase<TrafficControlConfig> {
    CONFIG_OBJ(read, HotLoadOperationConcurrency);
    CONFIG_OBJ(write, OperationConcurrency);
    CONFIG_OBJ(query, HotLoadOperationConcurrency);
    CONFIG_OBJ(remove, OperationConcurrency);
    CONFIG_OBJ(truncate, OperationConcurrency);

   public:
    size_t max_concurrent_updates() const {
      return write().max_concurrent_requests() * write().max_batch_size() +
             remove().max_concurrent_requests() * remove().max_batch_size() +
             truncate().max_concurrent_requests() * truncate().max_batch_size();
    }
  };

  class Config : public hf3fs::ConfigBase<Config> {
   public:
    CONFIG_OBJ(net_client, hf3fs::net::Client::Config);
    CONFIG_OBJ(net_client_for_updates, hf3fs::net::Client::Config);
    CONFIG_OBJ(retry, RetryConfig);
    CONFIG_OBJ(traffic_control, TrafficControlConfig);
    CONFIG_ITEM(implementation_type, ImplementationType::RPC);
    CONFIG_ITEM(chunk_checksum_type, ChecksumType::CRC32C);
    CONFIG_ITEM(create_net_client_for_updates, false);
    CONFIG_HOT_UPDATED_ITEM(check_overlapping_read_buffers, true);
    CONFIG_HOT_UPDATED_ITEM(check_overlapping_write_buffers, false);
    CONFIG_HOT_UPDATED_ITEM(max_inline_read_bytes, Size{0});
    CONFIG_HOT_UPDATED_ITEM(max_inline_write_bytes, Size{0});
    CONFIG_HOT_UPDATED_ITEM(max_read_io_bytes, Size{0});
  };

 public:
  StorageClient(const ClientId &clientId, const Config &config)
      : clientId_(clientId),
        config_(config) {}

  StorageClient()
      : StorageClient(ClientId::random(), kDefaultConfig) {}

  virtual ~StorageClient() = default;

  static std::shared_ptr<StorageClient> create(ClientId clientId,
                                               const Config &config,
                                               hf3fs::client::ICommonMgmtdClient &mgmtdClient);

  virtual hf3fs::client::ICommonMgmtdClient &getMgmtdClient() = 0;

  virtual Result<Void> start() { return Void{}; }

  // If the user does not call `stop()', the client is stopped in destructor.
  virtual void stop() {}

  /* Read `length' bytes from `offset' of the chunk.
     The memory pointed by `data' should be large enough to store the data, fall in the range of
     the registered `buffer' and does not overlap with other IOs in the same batch (can be disable by
     setting `check_overlapping_read_buffers').
   */
  virtual ReadIO createReadIO(ChainId chainId,
                              const ChunkId &chunkId,
                              uint32_t offset,
                              uint32_t length,
                              uint8_t *data,
                              IOBuffer *buffer,
                              void *userCtx = nullptr);

  /* Write `length' bytes of data at `offset' of the chunk.
     The memory pointed by `data' should be large enough to store the data, fall in the range of
     the registered `buffer' and does not overlap with other IOs in the same batch (can be disable by
     setting `check_overlapping_write_buffers').
     If option `chunk_checksum_type' is not none, a checksum will be calculated for the write buffer.
   */
  virtual WriteIO createWriteIO(ChainId chainId,
                                const ChunkId &chunkId,
                                uint32_t offset,
                                uint32_t length,
                                uint32_t chunkSize,
                                uint8_t *data,
                                IOBuffer *buffer,
                                void *userCtx = nullptr);

  /* Query the chunk with largest lexicographical id in range [chunkIdBegin, chunkIdEnd).
     `totalChunkLen' and `totalNumChunks' of chunks in the range are calculated and included in
     `QueryLastChunkResult'.
     If `moreChunksInRange' in `QueryLastChunkResult' is true, there exist more than
     `maxNumChunkIdsToProcess' chunks in range [chunkIdBegin, chunkIdEnd).
  */
  virtual QueryLastChunkOp createQueryOp(ChainId chainId,
                                         ChunkId chunkIdBegin,
                                         ChunkId chunkIdEnd,
                                         uint32_t maxNumChunkIdsToProcess = 1,
                                         void *userCtx = nullptr);

  /* Remove chunks in the range [chunkIdBegin, chunkIdEnd).
     Note that the `numChunksRemoved' in `RemoveChunksResult' might be less or equal to
     the number of chunks actually removed by storage service if the request fails and is
     automatically retried until it succeeds.
     If `moreChunksInRange' in `RemoveChunksResult' is true, there exist more than
     `maxNumChunkIdsToProcess' chunks in range [chunkIdBegin, chunkIdEnd).
   */
  virtual RemoveChunksOp createRemoveOp(ChainId chainId,
                                        ChunkId chunkIdBegin,
                                        ChunkId chunkIdEnd,
                                        uint32_t maxNumChunkIdsToProcess = 1,
                                        void *userCtx = nullptr);

  /* Truncate the chunk to `chunkLen' and create the chunk if it does not exist.
     `chunkSize' must equal to the size when the chunk was created if it already exists.
     A chunk of size `chunkSize' is created if does not exist.
     If `onlyExtendChunk' = true, extend the chunk if its length is less than `chunkLen';
     noop if its length is already greater or equal to `chunkLen'.
     The truncated/extended chunk size is returned as `lengthInfo' in the IO result;
     the user should check if the chunk size is expected.
  */
  virtual TruncateChunkOp createTruncateOp(ChainId chainId,
                                           const ChunkId &chunkId,
                                           uint32_t chunkLen,
                                           uint32_t chunkSize,
                                           bool onlyExtendChunk = false,
                                           void *userCtx = nullptr);

  // delete the returned IOBuffer object to deregister the buffer
  virtual Result<IOBuffer> registerIOBuffer(uint8_t *buf, size_t len);

  virtual CoTryTask<void> batchRead(std::span<ReadIO> readIOs,
                                    const flat::UserInfo &userInfo,
                                    const ReadOptions &options = ReadOptions(),
                                    std::vector<ReadIO *> *failedIOs = nullptr) = 0;

  virtual CoTryTask<void> batchWrite(std::span<WriteIO> writeIOs,
                                     const flat::UserInfo &userInfo,
                                     const WriteOptions &options = WriteOptions(),
                                     std::vector<WriteIO *> *failedIOs = nullptr) = 0;

  virtual CoTryTask<void> read(ReadIO &readIO,
                               const flat::UserInfo &userInfo,
                               const ReadOptions &options = ReadOptions()) = 0;

  virtual CoTryTask<void> write(WriteIO &writeIO,
                                const flat::UserInfo &userInfo,
                                const WriteOptions &options = WriteOptions()) = 0;

  // the following interfaces are assumed to be used at server-side (e.g. in meta service)

  virtual CoTryTask<void> queryLastChunk(std::span<QueryLastChunkOp> ops,
                                         const flat::UserInfo &userInfo,
                                         const ReadOptions &options = ReadOptions(),
                                         std::vector<QueryLastChunkOp *> *failedOps = nullptr) = 0;

  virtual CoTryTask<void> removeChunks(std::span<RemoveChunksOp> ops,
                                       const flat::UserInfo &userInfo,
                                       const WriteOptions &options = WriteOptions(),
                                       std::vector<RemoveChunksOp *> *failedOps = nullptr) = 0;

  virtual CoTryTask<void> truncateChunks(std::span<TruncateChunkOp> ops,
                                         const flat::UserInfo &userInfo,
                                         const WriteOptions &options = WriteOptions(),
                                         std::vector<TruncateChunkOp *> *failedOps = nullptr) = 0;

  virtual CoTryTask<SpaceInfoRsp> querySpaceInfo(NodeId nodeId) = 0;

  virtual CoTryTask<CreateTargetRsp> createTarget(NodeId nodeId, const CreateTargetReq &req) = 0;

  virtual CoTryTask<OfflineTargetRsp> offlineTarget(NodeId nodeId, const OfflineTargetReq &req) = 0;

  virtual CoTryTask<RemoveTargetRsp> removeTarget(NodeId nodeId, const RemoveTargetReq &req) = 0;

  virtual CoTryTask<std::vector<Result<QueryChunkRsp>>> queryChunk(const QueryChunkReq &req) = 0;

  virtual CoTryTask<ChunkMetaVector> getAllChunkMetadata(const ChainId &chainId, const TargetId &targetId) = 0;

 protected:
  static const Config kDefaultConfig;
  const ClientId clientId_;
  const Config &config_;
  std::atomic_uint64_t nextRequestId_ = 1;
};

}  // namespace hf3fs::storage::client

FMT_BEGIN_NAMESPACE

template <>
struct formatter<hf3fs::storage::client::RoutingTarget> : formatter<std::string_view> {
  template <typename FormatContext>
  auto format(const hf3fs::storage::client::RoutingTarget &routingTarget, FormatContext &ctx) const {
    return fmt::format_to(ctx.out(),
                          "{}@{}@{}:{}@{}:{}#{}",
                          routingTarget.chainId,
                          routingTarget.chainVer,
                          routingTarget.routingInfoVer,
                          routingTarget.targetInfo.targetId,
                          routingTarget.targetInfo.nodeId,
                          routingTarget.channel.id,
                          routingTarget.channel.seqnum);
  }
};

FMT_END_NAMESPACE
