#pragma once

#include "common/net/Transport.h"
#include "common/utils/ConfigBase.h"
#include "common/utils/Coroutine.h"
#include "common/utils/Duration.h"
#include "common/utils/LockManager.h"
#include "common/utils/RobinHood.h"
#include "common/utils/Shards.h"
#include "common/utils/Size.h"
#include "fbs/storage/Common.h"
#include "storage/service/TargetMap.h"

namespace hf3fs::storage {

struct Components;
class StorageOperator;

// 可靠更新协调器：
// - 以 (ClientId, ChainId, ChannelId, seqnum) 建模更新会话，序列化同一 channel 的并发更新
// - 去重与缓存：重复请求直接返回已缓存结果；丢回包时可拾取上一成功的 updateVer
// - 协调执行：调用 StorageOperator 处理本地更新与链式前向；记录并缓存结果
// - 生命周期：支持停止前拒绝新请求；定期清理已过期的客户端缓存
class ReliableUpdate {
 public:
  struct Config : ConfigBase<Config> {
    CONFIG_HOT_UPDATED_ITEM(clean_up_expired_clients, false);
    CONFIG_HOT_UPDATED_ITEM(expired_clients_timeout, 1_h);
  };
  ReliableUpdate(const Config &config, Components &components)
      : config_(config),
        components_(components) {}

  // 执行可靠更新：校验 tag/channel；获取并锁定 channel；命中缓存则返回，否则运行更新并缓存结果
  CoTask<IOResult> update(ServiceRequestContext &requestCtx,
                          UpdateReq &req,
                          net::IBSocket *ibSocket,
                          TargetPtr &target);

  // 清理过期客户端：依据活跃会话列表与超时阈值，移除不活跃客户端的缓存
  Result<Void> cleanUpExpiredClients(const robin_hood::unordered_set<std::string> &activeClients);

  // 停止前标记拒绝新请求
  void beforeStop() { stopped_ = true; }

 private:
  ConstructLog<"storage::ReliableUpdate"> constructLog_;
  const Config &config_;
  Components &components_;
  std::atomic<bool> stopped_ = false;
  folly::coro::Mutex mutex_;

  struct ReqResult {
    SERDE_STRUCT_FIELD(channelSeqnum, ChannelSeqNum{0});
    SERDE_STRUCT_FIELD(requestId, RequestId{0});
    SERDE_STRUCT_FIELD(updateResult, IOResult{});
    SERDE_STRUCT_FIELD(succUpdateVer, ChunkVer{});
    SERDE_STRUCT_FIELD(generationId, uint32_t{});
  };

  struct ClientStatus {
    std::unordered_map<std::pair<ChainId, ChannelId>, ReqResult> channelMap;
    UtcTime lastUsedTime;
  };
  using ClientMap = std::unordered_map<ClientId, std::shared_ptr<ClientStatus>>;
  Shards<ClientMap, 1024> shards_;
};

}  // namespace hf3fs::storage
