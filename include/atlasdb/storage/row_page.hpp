#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "atlasdb/storage/page.hpp"

namespace atlasdb::storage {

inline constexpr std::uint16_t kRowPageFormatVersion = 1U;
inline constexpr std::size_t kRowPageHeaderSize = 8U;
inline constexpr std::size_t kRowPageSlotSize = 4U;

struct RowPageStatus {
  bool ok;
  std::string code;
  std::string message;

  static RowPageStatus Ok(std::string message = {});
  static RowPageStatus Error(std::string code, std::string message);
};

[[nodiscard]] RowPageStatus InitializeRowPage(Page* page);
[[nodiscard]] RowPageStatus AppendRowToPage(Page* page,
                                            const std::vector<std::uint8_t>& row_bytes,
                                            std::uint16_t* out_slot_index);
[[nodiscard]] RowPageStatus ReadRowFromPage(const Page& page,
                                            std::uint16_t slot_index,
                                            std::vector<std::uint8_t>* out_row_bytes);
[[nodiscard]] RowPageStatus GetRowCountFromPage(const Page& page, std::uint16_t* out_row_count);

}  // namespace atlasdb::storage
