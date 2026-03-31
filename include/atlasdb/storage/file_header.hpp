#pragma once

#include <cstdint>
#include <string>

#include "atlasdb/storage/page.hpp"

namespace atlasdb::storage {

inline constexpr std::uint32_t kFileFormatVersion = 1;

struct HeaderStatus {
  bool ok;
  std::string code;
  std::string message;

  static HeaderStatus Ok(std::string message = {});
  static HeaderStatus Error(std::string code, std::string message);
};

struct DatabaseFileHeader {
  std::uint32_t format_version{kFileFormatVersion};
  std::uint32_t page_size{static_cast<std::uint32_t>(kPageSize)};
  std::uint32_t page_count{1};
  std::uint32_t catalog_root_page{0};
  std::uint64_t schema_epoch{0};
  std::uint64_t checkpoint_lsn{0};
};

[[nodiscard]] Page SerializeFileHeaderPage(const DatabaseFileHeader& header);
[[nodiscard]] HeaderStatus DeserializeFileHeaderPage(const Page& page, DatabaseFileHeader* out_header);

}  // namespace atlasdb::storage
