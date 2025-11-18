#pragma once

#include <folly/Range.h>

#include "common/utils/Result.h"
#include "storage/aio/BatchReadJob.h"
#include "storage/store/ChunkMetadata.h"
#include "storage/store/ChunkStore.h"
#include "storage/update/UpdateJob.h"

namespace hf3fs::storage {

// C++ 实现的本地副本存储路径；在未启用 Rust ChunkEngine 时承担 chunk 的读/写/提交。
// 读：aioPrepareRead/aioFinishRead 读取并校验元数据（commitVer/updateVer、长度、校验值）。
// 写：update 处理创建/扩展/截断/填零/正常写入，维护版本与状态（DIRTY/CLEAN），并更新校验。
// 提交：commit 校验链版本与提交版本，推进到 COMMIT 状态并持久化或删除。
class ChunkReplica {
 public:
  // 准备异步读：加载元数据，填充 commitVer/updateVer/chainVer、chunk 长度与校验；设置对齐偏移/长度与 FD
  static Result<Void> aioPrepareRead(ChunkStore &store, AioReadJob &job);

  // 完成异步读：再次校验元数据版本一致性，避免读到未提交或过期数据
  static Result<Void> aioFinishRead(ChunkStore &store, AioReadJob &job);

  // 执行写入：支持创建/删除/截断/扩展/顺序及随机写；推进 updateVer/chainVer；维护 DIRTY/CLEAN；更新校验
  static Result<uint32_t> update(ChunkStore &store, UpdateJob &job, folly::CPUThreadPoolExecutor &executor);

  // 更新校验：根据写入场景复用/组合/或重算校验（前缀/后缀），保持 chunk 的校验值一致
  static Result<Void> updateChecksum(ChunkInfo &chunkInfo,
                                     UpdateIO writeIO,
                                     uint32_t chunkSizeBeforeWrite,
                                     bool isAppendWrite);

  // 提交版本：校验链版本与提交版本，推进到 COMMIT 状态并持久化或删除
  static Result<uint32_t> commit(ChunkStore &store, UpdateJob &job);
};

}  // namespace hf3fs::storage
