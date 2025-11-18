#pragma once

#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <folly/Hash.h>
#include <folly/lang/Bits.h>
#include <string>

#include "common/serde/BigEndian.h"
#include "common/serde/Serde.h"
#include "common/utils/Int128.h"
#include "common/utils/Result.h"
#include "common/utils/Size.h"
#include "common/utils/StrongType.h"
#include "fbs/storage/Common.h"
#include "storage/store/ChunkFileView.h"

namespace hf3fs::storage {

// chunk size 最大为 64MB, 应该是 512K 倍增到这个 chunk size 的?
inline constexpr auto kMaxChunkSize = 64_MB;

// ChunkInfo 将 Chunk 的元信息与其所在物理文件的视图组合在一起，用于读写与查询
// - ChunkMetadata：记录版本、长度、状态、校验和，以及内部定位信息（chunk_size、文件索引、文件内偏移）
// - ChunkFileView：封装打开的文件描述符（普通/Direct IO）以及索引，并提供 read/write/checksum 等操作
struct ChunkInfo {
  ChunkMetadata meta;  // 元数据，包含版本、长度、状态、校验和、内部文件位置等
  ChunkFileView view;  // 文件视图，持有 fd 与索引，用于实际 IO 与校验
};

struct ChunkPosition {
  SERDE_STRUCT_FIELD(fileIdx, uint32_t{});
  SERDE_STRUCT_FIELD(offset, serde::BigEndian<std::size_t>{});
};

void reportFatalEvent();

}  // namespace hf3fs::storage

template <>
struct ::std::hash<hf3fs::storage::ChunkFileId> {
  size_t operator()(hf3fs::storage::ChunkFileId id) const {
    return folly::hash::twang_mix64(reinterpret_cast<uint64_t &>(id));
  }
};
