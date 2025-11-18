#pragma once

#include <folly/AtomicUnorderedMap.h>
#include <memory>
#include <queue>

#include "common/utils/ConfigBase.h"
#include "common/utils/Path.h"
#include "common/utils/UtcTime.h"
#include "kv/KVStore.h"
#include "storage/store/ChunkFileStore.h"
#include "storage/store/ChunkMetadata.h"
#include "storage/store/PhysicalConfig.h"

namespace hf3fs::storage {

// 管理 chunk 元数据的持久化与生命周期：封装 KV 元数据存储与物理文件存储的协作。
// 职责包括：创建/加载/迁移 meta kv，分配与回收物理 chunk 空间，读写/删除元数据，统计与迭代。
class ChunkMetaStore {
 public:
  class Config : public ConfigBase<Config> {
    // 预分配的物理空间大小（必须是 kMaxChunkSize 的倍数），用于批量创建物理 chunk 文件
    CONFIG_HOT_UPDATED_ITEM(allocate_size, 256_MB, [](Size s) { return s && s % kMaxChunkSize == 0; });
    // 回收批处理大小：每批回收的已删除 chunk 数量
    CONFIG_HOT_UPDATED_ITEM(recycle_batch_size, 256u, ConfigCheckers::checkPositive);
    // 打洞批处理大小：每批 punch hole 的块数量
    CONFIG_HOT_UPDATED_ITEM(punch_hole_batch_size, 16u, ConfigCheckers::checkPositive);
    // 标记为已删除的 chunk 的过期时间（超过后进入回收流程）
    CONFIG_HOT_UPDATED_ITEM(removed_chunk_expiration_time, 3_d);
    // 强制回收已删除 chunk 的时间（超过后即使空间紧张也强制回收）
    CONFIG_HOT_UPDATED_ITEM(removed_chunk_force_recycled_time, 1_h);
  };

  ChunkMetaStore(const Config &config, ChunkFileStore &fileStore)
      : config_(config),
        fileStore_(fileStore),
        allocateState_(16) {}

  ~ChunkMetaStore();

  // create chunk meta store.
  // 初始化并创建 meta kv 结构与哨兵键，绑定目标物理盘的路径/配置信息
  Result<Void> create(const kv::KVStore::Config &config, const PhysicalConfig &targetConfig);

  // load chunk meta store.
  // 加载已有的 meta kv（可选：不存在时创建），恢复分配状态与用量统计
  Result<Void> load(const kv::KVStore::Config &config,
                    const PhysicalConfig &targetConfig,
                    bool createIfMissing = false);

  // add new chunk size.
  // 注册支持的 chunk 尺寸，用于构建/维护对应尺寸的分配池与统计
  Result<Void> addChunkSize(const std::vector<Size> &sizeList);

  // migrate chunk meta store.
  // 迁移 meta kv 到新的物理配置（路径/磁盘），保持元数据一致与哨兵校验
  Result<Void> migrate(const kv::KVStore::Config &config, const PhysicalConfig &targetConfig);

  // get metadata of chunk. [thread-safe]
  // 从 meta kv 读取指定 chunk 的元数据（commit/update 版本、大小、校验、链版本等）
  Result<Void> get(const ChunkId &chunkId, ChunkMetadata &meta);

  // set metadata of chunk. [thread-safe]
  // 写入/更新指定 chunk 的元数据到 meta kv，并维护用量/状态统计
  Result<Void> set(const ChunkId &chunkId, const ChunkMetadata &meta);

  // remove metadata of chunk. [thread-safe]
  // 从 meta kv 删除指定 chunk 的元数据，记录删除时间以便后续回收
  Result<Void> remove(const ChunkId &chunkId, const ChunkMetadata &meta);

  // create a chunk. [thread-safe]
  // 创建新 chunk：分配物理位置（ChunkFileStore），初始化并写入 meta kv；支持批量预分配与线程池执行
  Result<Void> createChunk(const ChunkId &chunkId,
                           ChunkMetadata &meta,
                           uint32_t chunkSize,
                           folly::CPUThreadPoolExecutor &executor,
                           bool allowToAllocate);

  // recycle a batch of chunks, return true if has more. [thread-safe]
  // 回收一批已删除并过期的 chunk：打洞/释放空间，返回是否仍有待回收项
  Result<bool> punchHole();

  // sync the LOG of kv.
  // 同步/刷写 meta kv 的日志（确保持久化完整性）
  Result<Void> sync();

  // get used size.
  uint64_t usedSize() const { return std::max(int64_t(createdSize_.load() - removedSize_.load()), 0l); }

  // get reserved and unrecycled size.
  // 获取保留空间与未回收空间的统计（用于容量监控与策略决策）
  Result<Void> unusedSize(int64_t &reservedSize, int64_t &unrecycledSize);

  // get all uncommitted chunk ids.
  // 返回未提交状态的 chunk 列表（用于重置或巡检）
  auto &uncommitted() { return uncommitted_; }

  // enable or disable emergency recycling.
  // 启用/禁用紧急回收模式（在空间压力下加速回收）
  void setEmergencyRecycling(bool enable) { emergencyRecycling_ = enable; }

  // iterator.
  class Iterator {
   public:
    // 构造迭代器：包裹 KVStore 的迭代器，支持按 chunkId 前缀遍历
    explicit Iterator(kv::KVStore::IteratorPtr it, std::string_view chunkIdPrefix);
    // seek a chunk id prefix.
    // 定位到指定前缀的起始位置
    void seek(std::string_view chunkIdPrefix);
    // return valid or not.
    // 当前迭代位置是否有效
    bool valid() const;
    // get current chunk id.
    // 返回当前项的 chunkId
    ChunkId chunkId() const;
    // get current metadata.
    // 返回当前项的元数据（从 KV 解析）
    Result<ChunkMetadata> meta() const;
    // next metadata.
    // 迭代到下一项
    void next();
    // check status.
    // 检查迭代器状态（KV 层返回错误）
    Result<Void> status() const;

   private:
    kv::KVStore::IteratorPtr it_;
  };
  // 创建迭代器（可选前缀过滤）以遍历所有/部分 chunk 元数据
  Result<Iterator> iterator(std::string_view chunkIdPrefix = {});

 protected:
  // 校验哨兵键，确保 KV 初始化与版本兼容
  Result<Void> checkSentinel(std::string_view key);

  // 从 KV 读取容量/计数类指标键，更新统计字段
  Result<Void> getSize(std::string_view key, std::atomic<uint64_t> &size);

  struct AllocateState {
    // 分配/创建/回收控制的内部状态，用于同一 chunk 尺寸下的批量预分配与回收
    std::mutex createMutex;
    std::mutex recycleMutex;
    std::mutex allocateMutex;
    std::atomic<bool> loaded{};
    std::atomic<bool> allocating{};
    std::atomic<bool> recycling{};
    uint32_t chunkSize{};
    uint32_t allocateIndex{};               // createMutex.
    std::atomic<uint64_t> startingPoint{};  // createMutex.
    std::atomic<uint64_t> createdCount{};   // createMutex.
    std::atomic<uint64_t> usedCount{};      // createMutex.
    std::atomic<uint64_t> removedCount{};
    std::atomic<uint64_t> recycledCount{};                 // recycleMutex
    std::atomic<uint64_t> reusedCount{};                   // createMutex
    std::atomic<uint64_t> holeCount{};                     // recycleMutex
    std::atomic<UtcTime> oldestRemovedTimestamp{};         // recycleMutex
    std::vector<ChunkPosition> createdChunks;              // createMutex
    std::vector<ChunkPosition> recycledChunks;             // createMutex
    robin_hood::unordered_map<uint32_t, size_t> fileSize;  // createMutex
  };
  // 为指定 chunk 尺寸初始化对应的分配状态（预分配池/统计）
  void createAllocateState(uint32_t chunkSize);

  // 加载指定 chunk 尺寸的分配状态，若不存在则返回错误
  Result<AllocateState *> loadAllocateState(uint32_t chunkSize);

  // 触发批量预分配物理 chunk 空间（可加锁），更新分配池与统计
  Result<Void> allocateChunks(AllocateState &state, bool withLock = false);

  // 判断是否需要回收已删除 chunk（依据过期/强制阈值与紧急标志）
  bool needRecycleRemovedChunks(AllocateState &state);

  // 执行已删除 chunk 的回收流程（可加锁）：更新空间与统计，并写入 KV
  Result<Void> recycleRemovedChunks(AllocateState &state, bool withLock = false);

  // 对已删除 chunk 执行打洞（punch hole），返回是否仍有更多可打洞项
  Result<bool> punchHoleRemovedChunks(AllocateState &state, uint64_t expirationUs);

 private:
  const Config &config_;
  ChunkFileStore &fileStore_;

  std::unique_ptr<kv::KVStore> kv_;
  std::string sentinel_;
  std::string kvName_;
  bool hasSentinel_ = false;
  uint32_t physicalFileCount_ = 256;

  std::atomic<uint64_t> createdSize_ = 0;
  std::atomic<uint64_t> removedSize_ = 0;
  std::vector<ChunkId> uncommitted_;

  std::atomic<bool> emergencyRecycling_ = false;

  folly::AtomicUnorderedInsertMap<uint32_t, std::unique_ptr<AllocateState>> allocateState_;
};

}  // namespace hf3fs::storage
