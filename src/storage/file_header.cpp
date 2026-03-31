#include "atlasdb/storage/file_header.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>

namespace atlasdb::storage {
namespace {

constexpr std::array<std::uint8_t, 8> kFileMagic = {
    static_cast<std::uint8_t>('A'),
    static_cast<std::uint8_t>('T'),
    static_cast<std::uint8_t>('L'),
    static_cast<std::uint8_t>('A'),
    static_cast<std::uint8_t>('S'),
    static_cast<std::uint8_t>('D'),
    static_cast<std::uint8_t>('B'),
    0U,
};

constexpr std::size_t kMagicOffset = 0U;
constexpr std::size_t kVersionOffset = 8U;
constexpr std::size_t kPageSizeOffset = 12U;
constexpr std::size_t kPageCountOffset = 16U;
constexpr std::size_t kCatalogRootOffset = 20U;
constexpr std::size_t kSchemaEpochOffset = 24U;
constexpr std::size_t kCheckpointLsnOffset = 32U;

void WriteUint32(Page* page, std::size_t offset, std::uint32_t value) {
  page->bytes[offset + 0U] = static_cast<std::uint8_t>(value & 0xFFU);
  page->bytes[offset + 1U] = static_cast<std::uint8_t>((value >> 8U) & 0xFFU);
  page->bytes[offset + 2U] = static_cast<std::uint8_t>((value >> 16U) & 0xFFU);
  page->bytes[offset + 3U] = static_cast<std::uint8_t>((value >> 24U) & 0xFFU);
}

void WriteUint64(Page* page, std::size_t offset, std::uint64_t value) {
  page->bytes[offset + 0U] = static_cast<std::uint8_t>(value & 0xFFU);
  page->bytes[offset + 1U] = static_cast<std::uint8_t>((value >> 8U) & 0xFFU);
  page->bytes[offset + 2U] = static_cast<std::uint8_t>((value >> 16U) & 0xFFU);
  page->bytes[offset + 3U] = static_cast<std::uint8_t>((value >> 24U) & 0xFFU);
  page->bytes[offset + 4U] = static_cast<std::uint8_t>((value >> 32U) & 0xFFU);
  page->bytes[offset + 5U] = static_cast<std::uint8_t>((value >> 40U) & 0xFFU);
  page->bytes[offset + 6U] = static_cast<std::uint8_t>((value >> 48U) & 0xFFU);
  page->bytes[offset + 7U] = static_cast<std::uint8_t>((value >> 56U) & 0xFFU);
}

std::uint32_t ReadUint32(const Page& page, std::size_t offset) {
  std::uint32_t value = 0U;
  value |= static_cast<std::uint32_t>(page.bytes[offset + 0U]);
  value |= static_cast<std::uint32_t>(page.bytes[offset + 1U]) << 8U;
  value |= static_cast<std::uint32_t>(page.bytes[offset + 2U]) << 16U;
  value |= static_cast<std::uint32_t>(page.bytes[offset + 3U]) << 24U;
  return value;
}

std::uint64_t ReadUint64(const Page& page, std::size_t offset) {
  std::uint64_t value = 0U;
  value |= static_cast<std::uint64_t>(page.bytes[offset + 0U]);
  value |= static_cast<std::uint64_t>(page.bytes[offset + 1U]) << 8U;
  value |= static_cast<std::uint64_t>(page.bytes[offset + 2U]) << 16U;
  value |= static_cast<std::uint64_t>(page.bytes[offset + 3U]) << 24U;
  value |= static_cast<std::uint64_t>(page.bytes[offset + 4U]) << 32U;
  value |= static_cast<std::uint64_t>(page.bytes[offset + 5U]) << 40U;
  value |= static_cast<std::uint64_t>(page.bytes[offset + 6U]) << 48U;
  value |= static_cast<std::uint64_t>(page.bytes[offset + 7U]) << 56U;
  return value;
}

bool HasExpectedMagic(const Page& page) {
  for (std::size_t index = 0; index < kFileMagic.size(); ++index) {
    if (page.bytes[kMagicOffset + index] != kFileMagic[index]) {
      return false;
    }
  }
  return true;
}

}  // namespace

HeaderStatus HeaderStatus::Ok(std::string message) {
  return HeaderStatus{true, "", std::move(message)};
}

HeaderStatus HeaderStatus::Error(std::string code, std::string message) {
  return HeaderStatus{false, std::move(code), std::move(message)};
}

Page SerializeFileHeaderPage(const DatabaseFileHeader& header) {
  Page page = CreateZeroedPage(0U);

  for (std::size_t index = 0; index < kFileMagic.size(); ++index) {
    page.bytes[kMagicOffset + index] = kFileMagic[index];
  }

  WriteUint32(&page, kVersionOffset, header.format_version);
  WriteUint32(&page, kPageSizeOffset, header.page_size);
  WriteUint32(&page, kPageCountOffset, header.page_count);
  WriteUint32(&page, kCatalogRootOffset, header.catalog_root_page);
  WriteUint64(&page, kSchemaEpochOffset, header.schema_epoch);
  WriteUint64(&page, kCheckpointLsnOffset, header.checkpoint_lsn);

  return page;
}

HeaderStatus DeserializeFileHeaderPage(const Page& page, DatabaseFileHeader* out_header) {
  if (out_header == nullptr) {
    return HeaderStatus::Error("E3000", "output header pointer is null");
  }

  if (!HasExpectedMagic(page)) {
    return HeaderStatus::Error("E3001", "invalid database file magic");
  }

  DatabaseFileHeader header;
  header.format_version = ReadUint32(page, kVersionOffset);
  header.page_size = ReadUint32(page, kPageSizeOffset);
  header.page_count = ReadUint32(page, kPageCountOffset);
  header.catalog_root_page = ReadUint32(page, kCatalogRootOffset);
  header.schema_epoch = ReadUint64(page, kSchemaEpochOffset);
  header.checkpoint_lsn = ReadUint64(page, kCheckpointLsnOffset);

  if (header.format_version != kFileFormatVersion) {
    return HeaderStatus::Error("E3002", "unsupported file format version");
  }

  if (header.page_size != static_cast<std::uint32_t>(kPageSize)) {
    return HeaderStatus::Error("E3003", "unsupported page size in file header");
  }

  if (header.page_count == 0U) {
    return HeaderStatus::Error("E3004", "invalid page count in file header");
  }

  if (header.catalog_root_page != 0U && header.catalog_root_page >= header.page_count) {
    return HeaderStatus::Error("E3005", "catalog root page is outside declared page count");
  }

  *out_header = header;
  return HeaderStatus::Ok("valid file header");
}

}  // namespace atlasdb::storage
