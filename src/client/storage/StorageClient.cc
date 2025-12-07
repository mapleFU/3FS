#include <boost/core/ignore_unused.hpp>

#include "StorageClientImpl.h"
#include "StorageClientInMem.h"
#include "common/monitor/ScopedMetricsWriter.h"
#include "common/net/ib/RDMABuf.h"

namespace hf3fs::storage::client {

/*
  StorageClient 顶层实现说明：
  - `create(...)`：根据实现类型（RPC/InMem）构造具体客户端，并在创建后立即启动；
    同时校验更新并发配置是否超过通道分配器的上限。
  - IO 构造函数：仅封装参数与上下文，不做路由选择；实际路由在实现类中完成。
  - `registerIOBuffer(...)`：将用户缓冲区注册为 RDMA 缓冲，便于零拷贝；同时记录注册的成功/失败、时延与大小分布指标。
*/

static monitor::CountRecorder iobuf_reg_success_ops{"storage_client.iobuf_reg.success_ops"};
static monitor::CountRecorder iobuf_reg_failed_ops{"storage_client.iobuf_reg.failed_ops"};
static monitor::LatencyRecorder iobuf_reg_latency{"storage_client.iobuf_reg.latency"};
static monitor::DistributionRecorder iobuf_reg_size{"storage_client.iobuf_reg.size"};

const StorageClient::Config StorageClient::kDefaultConfig;

std::shared_ptr<StorageClient> StorageClient::create(ClientId clientId,
                                                     const Config &config,
                                                     hf3fs::client::ICommonMgmtdClient &mgmtdClient) {
  const auto &trafficControl = config.traffic_control();

  if (trafficControl.max_concurrent_updates() > UpdateChannelAllocator::kMaxNumChannels) {
    XLOGF(CRITICAL,
          "Bad config: trafficControl.max_concurrent_updates {} > UpdateChannelAllocator::kMaxNumChannels {}",
          trafficControl.max_concurrent_updates(),
          UpdateChannelAllocator::kMaxNumChannels);
    return nullptr;
  }

  std::shared_ptr<StorageClient> client;

  if (config.implementation_type() == ImplementationType::RPC) {
    client = std::make_shared<StorageClientImpl>(clientId, config, mgmtdClient);
  } else if (config.implementation_type() == ImplementationType::InMem) {
    client = std::make_shared<StorageClientInMem>(clientId, config, mgmtdClient);
  }

  if (!client || !client->start()) {
    XLOGF(CRITICAL,
          "Failed to create and start storage client of type {}",
          magic_enum::enum_name(config.implementation_type()));
    client.reset();
  }

  return client;
}

/*
  构造一次读操作的 IO：
  - 仅保存链/块/范围与用户缓冲区信息；路由目标与并发控制在实现类中处理。
*/
ReadIO StorageClient::createReadIO(ChainId chainId,
                                   const ChunkId &chunkId,
                                   uint32_t offset,
                                   uint32_t length,
                                   uint8_t *data,
                                   IOBuffer *buffer,
                                   void *userCtx) {
  return ReadIO{chainId, chunkId, offset, length, data, buffer, userCtx};
}

/*
  构造一次写操作的 IO：
  - 生成客户端请求号 `requestId`，后续用于日志与去重；
  - 写操作需要提供 `chunkSize` 以便服务端校验边界与创建块。
*/
WriteIO StorageClient::createWriteIO(ChainId chainId,
                                     const ChunkId &chunkId,
                                     uint32_t offset,
                                     uint32_t length,
                                     uint32_t chunkSize,
                                     uint8_t *data,
                                     IOBuffer *buffer,
                                     void *userCtx) {
  RequestId requestId(nextRequestId_.fetch_add(1));
  return WriteIO{requestId, chainId, chunkId, offset, length, chunkSize, data, buffer, userCtx};
}

/*
  构造区间查询操作：
  - 查询区间内按字典序最大的块，并返回区间统计信息。
*/
QueryLastChunkOp StorageClient::createQueryOp(ChainId chainId,
                                              ChunkId chunkIdBegin,
                                              ChunkId chunkIdEnd,
                                              uint32_t maxNumChunkIdsToProcess,
                                              void *userCtx) {
  return QueryLastChunkOp{chainId, {chunkIdBegin, chunkIdEnd, maxNumChunkIdsToProcess}, userCtx};
}

/*
  构造区间删除操作：
  - 生成请求号并封装区间范围；实际通道分配与并发控制由实现类完成。
*/
RemoveChunksOp StorageClient::createRemoveOp(ChainId chainId,
                                             ChunkId chunkIdBegin,
                                             ChunkId chunkIdEnd,
                                             uint32_t maxNumChunkIdsToProcess,
                                             void *userCtx) {
  RequestId requestId(nextRequestId_.fetch_add(1));
  return RemoveChunksOp{requestId, chainId, {chunkIdBegin, chunkIdEnd, maxNumChunkIdsToProcess}, userCtx};
}

/*
  构造截断/扩展操作：
  - 提供目标长度与块大小，`onlyExtendChunk` 控制仅扩展不缩短的行为。
*/
TruncateChunkOp StorageClient::createTruncateOp(ChainId chainId,
                                                const ChunkId &chunkId,
                                                uint32_t chunkLen,
                                                uint32_t chunkSize,
                                                bool onlyExtendChunk,
                                                void *userCtx) {
  RequestId requestId(nextRequestId_.fetch_add(1));
  return TruncateChunkOp(requestId, chainId, chunkId, chunkLen, chunkSize, onlyExtendChunk, userCtx);
}

/*
  注册用户缓冲为 RDMA IOBuffer：
  - 成功时返回 `IOBuffer`，失败时返回内存错误；
  - 记录注册成功/失败次数与时延、缓冲大小分布，便于性能分析。
*/
Result<IOBuffer> StorageClient::registerIOBuffer(uint8_t *buf, size_t len) {
  monitor::ScopedLatencyWriter latencyWriter(iobuf_reg_latency);
  iobuf_reg_size.addSample(len);

  auto rdmabuf = hf3fs::net::RDMABuf::createFromUserBuffer(buf, len);

  if (rdmabuf.valid()) {
    iobuf_reg_success_ops.addSample(1);
    return IOBuffer{rdmabuf};
  } else {
    iobuf_reg_failed_ops.addSample(1);
    return makeError(StorageClientCode::kMemoryError);
  }
}

}  // namespace hf3fs::storage::client
