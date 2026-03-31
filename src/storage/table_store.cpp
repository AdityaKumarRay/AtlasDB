#include "atlasdb/storage/table_store.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <utility>
#include <vector>

namespace atlasdb::storage {
namespace {

constexpr std::array<std::uint8_t, 8> kTableDirectoryMagic = {
    static_cast<std::uint8_t>('A'),
    static_cast<std::uint8_t>('T'),
    static_cast<std::uint8_t>('L'),
    static_cast<std::uint8_t>('T'),
    static_cast<std::uint8_t>('D'),
    static_cast<std::uint8_t>('I'),
    static_cast<std::uint8_t>('R'),
    0U,
};
constexpr std::uint16_t kTableDirectoryVersion = 1U;

constexpr std::size_t kMagicOffset = 0U;
constexpr std::size_t kVersionOffset = 8U;
constexpr std::size_t kDataPageCountOffset = 10U;
constexpr std::size_t kRowCountOffset = 12U;
constexpr std::size_t kEntriesOffset = 16U;
constexpr std::size_t kEntrySize = 4U;

constexpr std::size_t kMaxDirectoryEntries = (kPageSize - kEntriesOffset) / kEntrySize;

struct DirectoryState {
  Page page;
  std::uint16_t data_page_count{0U};
  std::uint32_t row_count{0U};
  std::vector<std::uint32_t> data_page_ids;
};

void WriteUint16(Page* page, std::size_t offset, std::uint16_t value) {
  page->bytes[offset + 0U] = static_cast<std::uint8_t>(value & 0xFFU);
  page->bytes[offset + 1U] = static_cast<std::uint8_t>((value >> 8U) & 0xFFU);
}

void WriteUint32(Page* page, std::size_t offset, std::uint32_t value) {
  page->bytes[offset + 0U] = static_cast<std::uint8_t>(value & 0xFFU);
  page->bytes[offset + 1U] = static_cast<std::uint8_t>((value >> 8U) & 0xFFU);
  page->bytes[offset + 2U] = static_cast<std::uint8_t>((value >> 16U) & 0xFFU);
  page->bytes[offset + 3U] = static_cast<std::uint8_t>((value >> 24U) & 0xFFU);
}

std::uint16_t ReadUint16(const Page& page, std::size_t offset) {
  const std::uint16_t b0 = static_cast<std::uint16_t>(page.bytes[offset + 0U]);
  const std::uint16_t b1 = static_cast<std::uint16_t>(page.bytes[offset + 1U]) << 8U;
  return static_cast<std::uint16_t>(b0 | b1);
}

std::uint32_t ReadUint32(const Page& page, std::size_t offset) {
  const std::uint32_t b0 = static_cast<std::uint32_t>(page.bytes[offset + 0U]);
  const std::uint32_t b1 = static_cast<std::uint32_t>(page.bytes[offset + 1U]) << 8U;
  const std::uint32_t b2 = static_cast<std::uint32_t>(page.bytes[offset + 2U]) << 16U;
  const std::uint32_t b3 = static_cast<std::uint32_t>(page.bytes[offset + 3U]) << 24U;
  return b0 | b1 | b2 | b3;
}

TableStoreStatus WrapPagerStatus(const PagerStatus& status) {
  if (status.ok) {
    return TableStoreStatus::Ok();
  }

  return TableStoreStatus::Error("E3403", status.code + ": " + status.message);
}

void WriteDirectoryPage(const DirectoryState& state, Page* page) {
  page->bytes.fill(0U);

  for (std::size_t index = 0U; index < kTableDirectoryMagic.size(); ++index) {
    page->bytes[kMagicOffset + index] = kTableDirectoryMagic[index];
  }

  WriteUint16(page, kVersionOffset, kTableDirectoryVersion);
  WriteUint16(page, kDataPageCountOffset, state.data_page_count);
  WriteUint32(page, kRowCountOffset, state.row_count);

  for (std::size_t index = 0U; index < state.data_page_ids.size(); ++index) {
    WriteUint32(page, kEntriesOffset + index * kEntrySize, state.data_page_ids[index]);
  }
}

TableStoreStatus ParseDirectoryPage(const Page& page, const Pager& pager, DirectoryState* out_state) {
  if (out_state == nullptr) {
    return TableStoreStatus::Error("E3400", "output directory state pointer is null");
  }

  for (std::size_t index = 0U; index < kTableDirectoryMagic.size(); ++index) {
    if (page.bytes[kMagicOffset + index] != kTableDirectoryMagic[index]) {
      return TableStoreStatus::Error("E3402", "table directory page has invalid magic");
    }
  }

  const std::uint16_t version = ReadUint16(page, kVersionOffset);
  if (version != kTableDirectoryVersion) {
    return TableStoreStatus::Error("E3402", "table directory page has unsupported version");
  }

  const std::uint16_t data_page_count = ReadUint16(page, kDataPageCountOffset);
  const std::uint32_t row_count = ReadUint32(page, kRowCountOffset);

  if (data_page_count == 0U) {
    return TableStoreStatus::Error("E3402", "table directory page has zero data pages");
  }

  if (data_page_count > kMaxDirectoryEntries) {
    return TableStoreStatus::Error("E3402", "table directory page exceeds entry capacity");
  }

  DirectoryState state;
  state.page = page;
  state.data_page_count = data_page_count;
  state.row_count = row_count;
  state.data_page_ids.reserve(static_cast<std::size_t>(data_page_count));

  for (std::uint16_t index = 0U; index < data_page_count; ++index) {
    const std::uint32_t page_id = ReadUint32(page, kEntriesOffset + static_cast<std::size_t>(index) * kEntrySize);
    if (page_id == 0U || page_id >= pager.Header().page_count) {
      return TableStoreStatus::Error("E3402", "table directory has invalid data page id");
    }
    state.data_page_ids.push_back(page_id);
  }

  *out_state = std::move(state);
  return TableStoreStatus::Ok();
}

bool ContainsPageId(const DirectoryState& state, std::uint32_t page_id) {
  return std::find(state.data_page_ids.begin(), state.data_page_ids.end(), page_id) !=
         state.data_page_ids.end();
}

}  // namespace

TableStoreStatus TableStoreStatus::Ok(std::string message) {
  return TableStoreStatus{true, "", std::move(message)};
}

TableStoreStatus TableStoreStatus::Error(std::string code, std::string message) {
  return TableStoreStatus{false, std::move(code), std::move(message)};
}

TableStore::TableStore(Pager* pager) : pager_(pager) {}

TableStoreStatus TableStore::Initialize(std::uint32_t* out_root_page) {
  if (out_root_page == nullptr) {
    return TableStoreStatus::Error("E3400", "output root page pointer is null");
  }

  if (pager_ == nullptr || !pager_->IsOpen()) {
    return TableStoreStatus::Error("E3401", "table store pager is not open");
  }

  std::uint32_t root_page_id = 0U;
  const PagerStatus root_allocate_status = pager_->AllocatePage(&root_page_id);
  if (!root_allocate_status.ok) {
    return WrapPagerStatus(root_allocate_status);
  }

  std::uint32_t first_data_page_id = 0U;
  const PagerStatus data_allocate_status = pager_->AllocatePage(&first_data_page_id);
  if (!data_allocate_status.ok) {
    return WrapPagerStatus(data_allocate_status);
  }

  DirectoryState state;
  state.page = CreateZeroedPage(root_page_id);
  state.data_page_count = 1U;
  state.row_count = 0U;
  state.data_page_ids.push_back(first_data_page_id);
  WriteDirectoryPage(state, &state.page);

  const PagerStatus write_directory_status = pager_->WritePage(state.page);
  if (!write_directory_status.ok) {
    return WrapPagerStatus(write_directory_status);
  }

  Page data_page = CreateZeroedPage(first_data_page_id);
  const RowPageStatus initialize_status = InitializeRowPage(&data_page);
  if (!initialize_status.ok) {
    return TableStoreStatus::Error(initialize_status.code, initialize_status.message);
  }

  const PagerStatus write_data_status = pager_->WritePage(data_page);
  if (!write_data_status.ok) {
    return WrapPagerStatus(write_data_status);
  }

  *out_root_page = root_page_id;
  return TableStoreStatus::Ok("initialized table store");
}

TableStoreStatus TableStore::AppendRow(std::uint32_t root_page_id,
                                       const std::vector<std::uint8_t>& row_bytes,
                                       TableRowLocation* out_location) {
  if (out_location == nullptr) {
    return TableStoreStatus::Error("E3400", "output row location pointer is null");
  }

  if (pager_ == nullptr || !pager_->IsOpen()) {
    return TableStoreStatus::Error("E3401", "table store pager is not open");
  }

  DirectoryState state;
  Page directory_page;
  const PagerStatus read_directory_status = pager_->ReadPage(root_page_id, &directory_page);
  if (!read_directory_status.ok) {
    return WrapPagerStatus(read_directory_status);
  }

  const TableStoreStatus parse_status = ParseDirectoryPage(directory_page, *pager_, &state);
  if (!parse_status.ok) {
    return parse_status;
  }

  const std::uint32_t last_page_id = state.data_page_ids.back();
  Page data_page;
  const PagerStatus read_data_status = pager_->ReadPage(last_page_id, &data_page);
  if (!read_data_status.ok) {
    return WrapPagerStatus(read_data_status);
  }

  std::uint16_t slot_index = 0U;
  RowPageStatus append_status = AppendRowToPage(&data_page, row_bytes, &slot_index);

  if (!append_status.ok && append_status.code == "E3303") {
    if (state.data_page_ids.size() >= kMaxDirectoryEntries) {
      return TableStoreStatus::Error("E3406", "table directory page is full");
    }

    std::uint32_t new_data_page_id = 0U;
    const PagerStatus allocate_status = pager_->AllocatePage(&new_data_page_id);
    if (!allocate_status.ok) {
      return WrapPagerStatus(allocate_status);
    }

    Page new_data_page = CreateZeroedPage(new_data_page_id);
    const RowPageStatus initialize_status = InitializeRowPage(&new_data_page);
    if (!initialize_status.ok) {
      return TableStoreStatus::Error(initialize_status.code, initialize_status.message);
    }

    append_status = AppendRowToPage(&new_data_page, row_bytes, &slot_index);
    if (!append_status.ok) {
      return TableStoreStatus::Error(append_status.code, append_status.message);
    }

    const PagerStatus write_new_data_status = pager_->WritePage(new_data_page);
    if (!write_new_data_status.ok) {
      return WrapPagerStatus(write_new_data_status);
    }

    state.data_page_ids.push_back(new_data_page_id);
    state.data_page_count = static_cast<std::uint16_t>(state.data_page_ids.size());

    state.row_count += 1U;
    WriteDirectoryPage(state, &state.page);
    const PagerStatus write_directory_status = pager_->WritePage(state.page);
    if (!write_directory_status.ok) {
      return WrapPagerStatus(write_directory_status);
    }

    *out_location = TableRowLocation{new_data_page_id, slot_index};
    return TableStoreStatus::Ok("appended row to table store");
  }

  if (!append_status.ok) {
    return TableStoreStatus::Error(append_status.code, append_status.message);
  }

  const PagerStatus write_data_status = pager_->WritePage(data_page);
  if (!write_data_status.ok) {
    return WrapPagerStatus(write_data_status);
  }

  state.row_count += 1U;
  WriteDirectoryPage(state, &state.page);
  const PagerStatus write_directory_status = pager_->WritePage(state.page);
  if (!write_directory_status.ok) {
    return WrapPagerStatus(write_directory_status);
  }

  *out_location = TableRowLocation{last_page_id, slot_index};
  return TableStoreStatus::Ok("appended row to table store");
}

TableStoreStatus TableStore::ReadRow(std::uint32_t root_page_id,
                                     const TableRowLocation& location,
                                     std::vector<std::uint8_t>* out_row_bytes) {
  if (out_row_bytes == nullptr) {
    return TableStoreStatus::Error("E3400", "output row bytes pointer is null");
  }

  if (pager_ == nullptr || !pager_->IsOpen()) {
    return TableStoreStatus::Error("E3401", "table store pager is not open");
  }

  DirectoryState state;
  Page directory_page;
  const PagerStatus read_directory_status = pager_->ReadPage(root_page_id, &directory_page);
  if (!read_directory_status.ok) {
    return WrapPagerStatus(read_directory_status);
  }

  const TableStoreStatus parse_status = ParseDirectoryPage(directory_page, *pager_, &state);
  if (!parse_status.ok) {
    return parse_status;
  }

  if (!ContainsPageId(state, location.page_id)) {
    return TableStoreStatus::Error("E3405", "row location page is not part of table store");
  }

  Page data_page;
  const PagerStatus read_data_status = pager_->ReadPage(location.page_id, &data_page);
  if (!read_data_status.ok) {
    return WrapPagerStatus(read_data_status);
  }

  const RowPageStatus read_row_status = ReadRowFromPage(data_page, location.slot_index, out_row_bytes);
  if (!read_row_status.ok) {
    return TableStoreStatus::Error(read_row_status.code, read_row_status.message);
  }

  return TableStoreStatus::Ok("read row from table store");
}

TableStoreStatus TableStore::ScanRows(std::uint32_t root_page_id,
                                      std::vector<StoredTableRow>* out_rows) {
  if (out_rows == nullptr) {
    return TableStoreStatus::Error("E3400", "output rows pointer is null");
  }

  if (pager_ == nullptr || !pager_->IsOpen()) {
    return TableStoreStatus::Error("E3401", "table store pager is not open");
  }

  DirectoryState state;
  Page directory_page;
  const PagerStatus read_directory_status = pager_->ReadPage(root_page_id, &directory_page);
  if (!read_directory_status.ok) {
    return WrapPagerStatus(read_directory_status);
  }

  const TableStoreStatus parse_status = ParseDirectoryPage(directory_page, *pager_, &state);
  if (!parse_status.ok) {
    return parse_status;
  }

  std::vector<StoredTableRow> rows;
  rows.reserve(static_cast<std::size_t>(state.row_count));

  for (const std::uint32_t page_id : state.data_page_ids) {
    Page data_page;
    const PagerStatus read_data_status = pager_->ReadPage(page_id, &data_page);
    if (!read_data_status.ok) {
      return WrapPagerStatus(read_data_status);
    }

    std::uint16_t page_row_count = 0U;
    const RowPageStatus page_count_status = GetRowCountFromPage(data_page, &page_row_count);
    if (!page_count_status.ok) {
      return TableStoreStatus::Error(page_count_status.code, page_count_status.message);
    }

    for (std::uint16_t slot_index = 0U; slot_index < page_row_count; ++slot_index) {
      std::vector<std::uint8_t> row_bytes;
      const RowPageStatus read_row_status = ReadRowFromPage(data_page, slot_index, &row_bytes);
      if (!read_row_status.ok) {
        return TableStoreStatus::Error(read_row_status.code, read_row_status.message);
      }

      rows.push_back(StoredTableRow{TableRowLocation{page_id, slot_index}, std::move(row_bytes)});
    }
  }

  if (rows.size() != static_cast<std::size_t>(state.row_count)) {
    return TableStoreStatus::Error("E3404", "table directory row count does not match scanned rows");
  }

  *out_rows = std::move(rows);
  return TableStoreStatus::Ok("scanned rows from table store");
}

TableStoreStatus TableStore::RowCount(std::uint32_t root_page_id, std::uint32_t* out_row_count) {
  if (out_row_count == nullptr) {
    return TableStoreStatus::Error("E3400", "output row count pointer is null");
  }

  if (pager_ == nullptr || !pager_->IsOpen()) {
    return TableStoreStatus::Error("E3401", "table store pager is not open");
  }

  DirectoryState state;
  Page directory_page;
  const PagerStatus read_directory_status = pager_->ReadPage(root_page_id, &directory_page);
  if (!read_directory_status.ok) {
    return WrapPagerStatus(read_directory_status);
  }

  const TableStoreStatus parse_status = ParseDirectoryPage(directory_page, *pager_, &state);
  if (!parse_status.ok) {
    return parse_status;
  }

  *out_row_count = state.row_count;
  return TableStoreStatus::Ok("row count available");
}

}  // namespace atlasdb::storage
