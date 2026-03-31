#include "atlasdb/storage/row_page.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <utility>
#include <vector>

namespace atlasdb::storage {
namespace {

constexpr std::size_t kVersionOffset = 0U;
constexpr std::size_t kRowCountOffset = 2U;
constexpr std::size_t kFreeStartOffset = 4U;
constexpr std::size_t kFreeEndOffset = 6U;

void WriteUint16(Page* page, std::size_t offset, std::uint16_t value) {
  page->bytes[offset + 0U] = static_cast<std::uint8_t>(value & 0xFFU);
  page->bytes[offset + 1U] = static_cast<std::uint8_t>((value >> 8U) & 0xFFU);
}

std::uint16_t ReadUint16(const Page& page, std::size_t offset) {
  const std::uint16_t b0 = static_cast<std::uint16_t>(page.bytes[offset + 0U]);
  const std::uint16_t b1 = static_cast<std::uint16_t>(page.bytes[offset + 1U]) << 8U;
  return static_cast<std::uint16_t>(b0 | b1);
}

RowPageStatus ValidateLayout(const Page& page,
                             std::uint16_t* out_row_count,
                             std::uint16_t* out_free_start,
                             std::uint16_t* out_free_end) {
  const std::uint16_t version = ReadUint16(page, kVersionOffset);
  if (version != kRowPageFormatVersion) {
    return RowPageStatus::Error("E3301", "row page has unsupported format version");
  }

  const std::uint16_t row_count = ReadUint16(page, kRowCountOffset);
  const std::uint16_t free_start = ReadUint16(page, kFreeStartOffset);
  const std::uint16_t free_end = ReadUint16(page, kFreeEndOffset);

  const std::size_t expected_free_start =
      kRowPageHeaderSize + static_cast<std::size_t>(row_count) * kRowPageSlotSize;

  if (expected_free_start > kPageSize) {
    return RowPageStatus::Error("E3301", "row page slot directory exceeds page size");
  }

  if (static_cast<std::size_t>(free_start) != expected_free_start) {
    return RowPageStatus::Error("E3301", "row page free-start pointer is inconsistent");
  }

  if (free_start > free_end || static_cast<std::size_t>(free_end) > kPageSize) {
    return RowPageStatus::Error("E3301", "row page free-space boundaries are invalid");
  }

  if (out_row_count != nullptr) {
    *out_row_count = row_count;
  }
  if (out_free_start != nullptr) {
    *out_free_start = free_start;
  }
  if (out_free_end != nullptr) {
    *out_free_end = free_end;
  }

  return RowPageStatus::Ok();
}

}  // namespace

RowPageStatus RowPageStatus::Ok(std::string message) {
  return RowPageStatus{true, "", std::move(message)};
}

RowPageStatus RowPageStatus::Error(std::string code, std::string message) {
  return RowPageStatus{false, std::move(code), std::move(message)};
}

RowPageStatus InitializeRowPage(Page* page) {
  if (page == nullptr) {
    return RowPageStatus::Error("E3300", "row page pointer is null");
  }

  page->bytes.fill(0U);
  WriteUint16(page, kVersionOffset, kRowPageFormatVersion);
  WriteUint16(page, kRowCountOffset, 0U);
  WriteUint16(page, kFreeStartOffset, static_cast<std::uint16_t>(kRowPageHeaderSize));
  WriteUint16(page, kFreeEndOffset, static_cast<std::uint16_t>(kPageSize));
  return RowPageStatus::Ok("initialized row page");
}

RowPageStatus AppendRowToPage(Page* page,
                              const std::vector<std::uint8_t>& row_bytes,
                              std::uint16_t* out_slot_index) {
  if (page == nullptr) {
    return RowPageStatus::Error("E3300", "row page pointer is null");
  }

  if (out_slot_index == nullptr) {
    return RowPageStatus::Error("E3300", "output slot pointer is null");
  }

  if (row_bytes.empty()) {
    return RowPageStatus::Error("E3306", "row payload is empty");
  }

  if (row_bytes.size() > static_cast<std::size_t>(std::numeric_limits<std::uint16_t>::max())) {
    return RowPageStatus::Error("E3302", "row payload exceeds slot size limit");
  }

  std::uint16_t row_count = 0U;
  std::uint16_t free_start = 0U;
  std::uint16_t free_end = 0U;
  const RowPageStatus layout_status = ValidateLayout(*page, &row_count, &free_start, &free_end);
  if (!layout_status.ok) {
    return layout_status;
  }

  if (row_count == std::numeric_limits<std::uint16_t>::max()) {
    return RowPageStatus::Error("E3302", "row count exceeds slot index space");
  }

  const std::size_t available_bytes = static_cast<std::size_t>(free_end - free_start);
  const std::size_t required_bytes = row_bytes.size() + kRowPageSlotSize;
  if (required_bytes > available_bytes) {
    return RowPageStatus::Error("E3303", "insufficient free space on row page");
  }

  const std::uint16_t row_size = static_cast<std::uint16_t>(row_bytes.size());
  const std::uint16_t row_offset = static_cast<std::uint16_t>(free_end - row_size);

  std::copy(row_bytes.begin(), row_bytes.end(),
            page->bytes.begin() + static_cast<std::ptrdiff_t>(row_offset));

  const std::size_t slot_offset = kRowPageHeaderSize + static_cast<std::size_t>(row_count) * kRowPageSlotSize;
  WriteUint16(page, slot_offset + 0U, row_offset);
  WriteUint16(page, slot_offset + 2U, row_size);

  WriteUint16(page, kRowCountOffset, static_cast<std::uint16_t>(row_count + 1U));
  WriteUint16(page, kFreeStartOffset,
              static_cast<std::uint16_t>(kRowPageHeaderSize +
                                         (static_cast<std::size_t>(row_count) + 1U) * kRowPageSlotSize));
  WriteUint16(page, kFreeEndOffset, row_offset);

  *out_slot_index = row_count;
  return RowPageStatus::Ok("appended row to page");
}

RowPageStatus ReadRowFromPage(const Page& page,
                              std::uint16_t slot_index,
                              std::vector<std::uint8_t>* out_row_bytes) {
  if (out_row_bytes == nullptr) {
    return RowPageStatus::Error("E3300", "output row buffer pointer is null");
  }

  std::uint16_t row_count = 0U;
  std::uint16_t free_end = 0U;
  const RowPageStatus layout_status = ValidateLayout(page, &row_count, nullptr, &free_end);
  if (!layout_status.ok) {
    return layout_status;
  }

  if (slot_index >= row_count) {
    return RowPageStatus::Error("E3304", "slot index is outside row count");
  }

  const std::size_t slot_offset = kRowPageHeaderSize + static_cast<std::size_t>(slot_index) * kRowPageSlotSize;
  const std::uint16_t row_offset = ReadUint16(page, slot_offset + 0U);
  const std::uint16_t row_size = ReadUint16(page, slot_offset + 2U);

  const std::size_t row_begin = static_cast<std::size_t>(row_offset);
  const std::size_t row_end = row_begin + static_cast<std::size_t>(row_size);
  if (row_begin < static_cast<std::size_t>(free_end) || row_end > kPageSize) {
    return RowPageStatus::Error("E3305", "row slot payload range is invalid");
  }

  out_row_bytes->assign(page.bytes.begin() + static_cast<std::ptrdiff_t>(row_begin),
                        page.bytes.begin() + static_cast<std::ptrdiff_t>(row_end));
  return RowPageStatus::Ok("read row from page");
}

RowPageStatus GetRowCountFromPage(const Page& page, std::uint16_t* out_row_count) {
  if (out_row_count == nullptr) {
    return RowPageStatus::Error("E3300", "output row count pointer is null");
  }

  std::uint16_t row_count = 0U;
  const RowPageStatus layout_status = ValidateLayout(page, &row_count, nullptr, nullptr);
  if (!layout_status.ok) {
    return layout_status;
  }

  *out_row_count = row_count;
  return RowPageStatus::Ok("row count available");
}

}  // namespace atlasdb::storage
