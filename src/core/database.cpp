#include "atlasdb/database.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "atlasdb/parser/ast.hpp"
#include "atlasdb/parser/parser.hpp"
#include "atlasdb/storage/row_codec.hpp"
#include "atlasdb/storage/table_store.hpp"
#include "atlasdb/version.hpp"

namespace atlasdb {
namespace {

constexpr std::array<std::uint8_t, 8> kCatalogSnapshotMagic = {
  static_cast<std::uint8_t>('A'),
  static_cast<std::uint8_t>('T'),
  static_cast<std::uint8_t>('L'),
  static_cast<std::uint8_t>('S'),
  static_cast<std::uint8_t>('N'),
  static_cast<std::uint8_t>('A'),
  static_cast<std::uint8_t>('P'),
  0U,
};
constexpr std::uint32_t kCatalogSnapshotVersion = 1U;
constexpr std::size_t kCatalogSnapshotHeaderSize = 16U;

std::string Trim(std::string_view input) {
  auto begin = input.begin();
  auto end = input.end();

  while (begin != end && std::isspace(static_cast<unsigned char>(*begin)) != 0) {
    ++begin;
  }

  while (begin != end && std::isspace(static_cast<unsigned char>(*(end - 1))) != 0) {
    --end;
  }

  return std::string(begin, end);
}

std::string NormalizeIdentifier(std::string_view identifier) {
  std::string normalized(identifier);
  for (char& value : normalized) {
    value = static_cast<char>(std::tolower(static_cast<unsigned char>(value)));
  }

  return normalized;
}

std::string EscapeSingleQuotes(std::string_view input) {
  std::string escaped;
  escaped.reserve(input.size());

  for (const char value : input) {
    if (value == '\'') {
      escaped.push_back('\'');
    }
    escaped.push_back(value);
  }

  return escaped;
}

std::string FormatLiteral(const parser::ValueLiteral& literal) {
  if (std::holds_alternative<std::int64_t>(literal.value)) {
    return std::to_string(std::get<std::int64_t>(literal.value));
  }

  const std::string escaped = EscapeSingleQuotes(std::get<std::string>(literal.value));
  return "'" + escaped + "'";
}

std::string FormatRowValues(const std::vector<parser::ValueLiteral>& row) {
  std::string output = "[";
  for (std::size_t index = 0; index < row.size(); ++index) {
    if (index != 0U) {
      output += ", ";
    }
    output += FormatLiteral(row[index]);
  }
  output += "]";
  return output;
}

std::string FormatRows(const std::vector<std::vector<parser::ValueLiteral>>& rows) {
  std::string output;
  for (std::size_t row_index = 0; row_index < rows.size(); ++row_index) {
    if (row_index != 0U) {
      output += "; ";
    }
    output += FormatRowValues(rows[row_index]);
  }
  return output;
}

void WriteUint32(std::vector<std::uint8_t>* bytes, std::uint32_t value) {
  bytes->push_back(static_cast<std::uint8_t>(value & 0xFFU));
  bytes->push_back(static_cast<std::uint8_t>((value >> 8U) & 0xFFU));
  bytes->push_back(static_cast<std::uint8_t>((value >> 16U) & 0xFFU));
  bytes->push_back(static_cast<std::uint8_t>((value >> 24U) & 0xFFU));
}

bool ReadUint32(const std::vector<std::uint8_t>& bytes, std::size_t offset, std::uint32_t* value) {
  if (value == nullptr) {
    return false;
  }

  if (offset + 4U > bytes.size()) {
    return false;
  }

  const std::uint32_t b0 = static_cast<std::uint32_t>(bytes[offset + 0U]);
  const std::uint32_t b1 = static_cast<std::uint32_t>(bytes[offset + 1U]) << 8U;
  const std::uint32_t b2 = static_cast<std::uint32_t>(bytes[offset + 2U]) << 16U;
  const std::uint32_t b3 = static_cast<std::uint32_t>(bytes[offset + 3U]) << 24U;
  *value = b0 | b1 | b2 | b3;
  return true;
}

}  // namespace

Status Status::Ok(std::string message) {
  return Status{true, std::move(message)};
}

Status Status::Error(std::string message) {
  return Status{false, std::move(message)};
}

DatabaseEngine::DatabaseEngine(std::string database_path) {
  if (database_path.empty()) {
    return;
  }

  persistence_enabled_ = true;
  pager_ = std::make_unique<storage::Pager>();

  const storage::PagerStatus open_status = pager_->Open(database_path);
  if (!open_status.ok) {
    startup_error_ = open_status.code + ": " + open_status.message;
    return;
  }

  schema_epoch_ = pager_->Header().schema_epoch;
  const Status load_status = LoadCatalogSnapshotFromPager();
  if (!load_status.ok) {
    startup_error_ = load_status.message;
    return;
  }

  const Status rebuild_status = RebuildTableStoresFromCatalog();
  if (!rebuild_status.ok) {
    startup_error_ = rebuild_status.message;
    return;
  }

  last_message_ = "AtlasDB initialized with persistent catalog";
}

Status DatabaseEngine::LoadCatalogSnapshotFromPager() {
  if (!persistence_enabled_ || pager_ == nullptr) {
    return Status::Ok();
  }

  const std::uint32_t catalog_root_page = pager_->Header().catalog_root_page;
  if (catalog_root_page == 0U) {
    return Status::Ok("no persisted catalog snapshot");
  }

  storage::Page first_page;
  const storage::PagerStatus read_first_status = pager_->ReadPage(catalog_root_page, &first_page);
  if (!read_first_status.ok) {
    return Status::Error(read_first_status.code + ": " + read_first_status.message);
  }

  for (std::size_t index = 0; index < kCatalogSnapshotMagic.size(); ++index) {
    if (first_page.bytes[index] != kCatalogSnapshotMagic[index]) {
      return Status::Error("E4001: invalid catalog snapshot magic");
    }
  }

  std::vector<std::uint8_t> header_bytes(first_page.bytes.begin(), first_page.bytes.begin() + kCatalogSnapshotHeaderSize);

  std::uint32_t version = 0U;
  if (!ReadUint32(header_bytes, 8U, &version)) {
    return Status::Error("E4001: truncated catalog snapshot header");
  }

  if (version != kCatalogSnapshotVersion) {
    return Status::Error("E4002: unsupported catalog snapshot version");
  }

  std::uint32_t payload_size = 0U;
  if (!ReadUint32(header_bytes, 12U, &payload_size)) {
    return Status::Error("E4001: truncated catalog snapshot header");
  }

  const std::size_t total_snapshot_size = kCatalogSnapshotHeaderSize + static_cast<std::size_t>(payload_size);
  const std::size_t pages_needed =
      (total_snapshot_size + storage::kPageSize - 1U) / storage::kPageSize;

  std::vector<std::uint8_t> snapshot_bytes(total_snapshot_size, 0U);

  for (std::size_t page_offset = 0U; page_offset < pages_needed; ++page_offset) {
    const std::uint64_t page_id_wide = static_cast<std::uint64_t>(catalog_root_page) +
                                       static_cast<std::uint64_t>(page_offset);
    if (page_id_wide > static_cast<std::uint64_t>(std::numeric_limits<std::uint32_t>::max())) {
      return Status::Error("E4004: catalog snapshot page id overflow");
    }

    storage::Page page;
    const storage::PagerStatus read_status =
        pager_->ReadPage(static_cast<std::uint32_t>(page_id_wide), &page);
    if (!read_status.ok) {
      return Status::Error(read_status.code + ": " + read_status.message);
    }

    const std::size_t begin = page_offset * storage::kPageSize;
    const std::size_t remaining = total_snapshot_size - begin;
    const std::size_t copy_size = std::min(remaining, storage::kPageSize);
    std::copy_n(page.bytes.begin(), static_cast<std::ptrdiff_t>(copy_size), snapshot_bytes.begin() +
                                                              static_cast<std::ptrdiff_t>(begin));
  }

  std::vector<std::uint8_t> payload_bytes;
  payload_bytes.reserve(static_cast<std::size_t>(payload_size));
  payload_bytes.insert(payload_bytes.end(), snapshot_bytes.begin() + static_cast<std::ptrdiff_t>(kCatalogSnapshotHeaderSize),
                       snapshot_bytes.end());

  const catalog::CatalogStatus deserialize_status = catalog_.Deserialize(payload_bytes);
  if (!deserialize_status.ok) {
    return Status::Error(deserialize_status.code + ": " + deserialize_status.message);
  }

  return Status::Ok("loaded catalog snapshot");
}

Status DatabaseEngine::PersistCatalogSnapshotToPager() {
  if (!persistence_enabled_ || pager_ == nullptr) {
    return Status::Ok();
  }

  std::vector<std::uint8_t> payload_bytes;
  const catalog::CatalogStatus serialize_status = catalog_.Serialize(&payload_bytes);
  if (!serialize_status.ok) {
    return Status::Error(serialize_status.code + ": " + serialize_status.message);
  }

  if (payload_bytes.size() > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
    return Status::Error("E4003: catalog snapshot exceeds supported payload size");
  }

  std::vector<std::uint8_t> snapshot_bytes;
  snapshot_bytes.reserve(kCatalogSnapshotHeaderSize + payload_bytes.size());
  snapshot_bytes.insert(snapshot_bytes.end(), kCatalogSnapshotMagic.begin(), kCatalogSnapshotMagic.end());
  WriteUint32(&snapshot_bytes, kCatalogSnapshotVersion);
  WriteUint32(&snapshot_bytes, static_cast<std::uint32_t>(payload_bytes.size()));
  snapshot_bytes.insert(snapshot_bytes.end(), payload_bytes.begin(), payload_bytes.end());

  const std::size_t pages_needed =
      (snapshot_bytes.size() + storage::kPageSize - 1U) / storage::kPageSize;

  std::vector<std::uint32_t> page_ids;
  page_ids.reserve(pages_needed);

  for (std::size_t page_index = 0U; page_index < pages_needed; ++page_index) {
    std::uint32_t page_id = 0U;
    const storage::PagerStatus allocate_status = pager_->AllocatePage(&page_id);
    if (!allocate_status.ok) {
      return Status::Error(allocate_status.code + ": " + allocate_status.message);
    }
    page_ids.push_back(page_id);
  }

  for (std::size_t page_index = 0U; page_index < pages_needed; ++page_index) {
    storage::Page page = storage::CreateZeroedPage(page_ids[page_index]);

    const std::size_t begin = page_index * storage::kPageSize;
    const std::size_t remaining = snapshot_bytes.size() - begin;
    const std::size_t copy_size = std::min(remaining, storage::kPageSize);

    std::copy_n(snapshot_bytes.begin() + static_cast<std::ptrdiff_t>(begin),
                static_cast<std::ptrdiff_t>(copy_size), page.bytes.begin());

    const storage::PagerStatus write_status = pager_->WritePage(page);
    if (!write_status.ok) {
      return Status::Error(write_status.code + ": " + write_status.message);
    }
  }

  schema_epoch_ += 1U;
  const storage::PagerStatus metadata_status =
      pager_->UpdateCatalogMetadata(page_ids.front(), schema_epoch_);
  if (!metadata_status.ok) {
    return Status::Error(metadata_status.code + ": " + metadata_status.message);
  }

  return Status::Ok("persisted catalog snapshot");
}

Status DatabaseEngine::RebuildTableStoresFromCatalog() {
  table_store_roots_.clear();

  if (!persistence_enabled_ || pager_ == nullptr) {
    return Status::Ok();
  }

  storage::TableStore table_store(pager_.get());
  const std::vector<catalog::TableSnapshot> tables = catalog_.SnapshotTables();

  for (const catalog::TableSnapshot& table : tables) {
    std::uint32_t root_page_id = 0U;
    const storage::TableStoreStatus init_status = table_store.Initialize(&root_page_id);
    if (!init_status.ok) {
      return Status::Error(init_status.code + ": " + init_status.message);
    }

    for (const std::vector<parser::ValueLiteral>& row : table.rows) {
      std::vector<std::uint8_t> row_bytes;
      const storage::RowCodecStatus encode_status =
          storage::SerializeRow(table.columns, row, &row_bytes);
      if (!encode_status.ok) {
        return Status::Error(encode_status.code + ": " + encode_status.message);
      }

      storage::TableRowLocation location;
      const storage::TableStoreStatus append_status =
          table_store.AppendRow(root_page_id, row_bytes, &location);
      if (!append_status.ok) {
        return Status::Error(append_status.code + ": " + append_status.message);
      }
    }

    table_store_roots_.emplace(NormalizeIdentifier(table.name), root_page_id);
  }

  return Status::Ok("rebuilt table-store pages from catalog snapshot");
}

Status DatabaseEngine::InitializeCreateTableStore(const parser::CreateTableStatement& statement) {
  if (!persistence_enabled_ || pager_ == nullptr) {
    return Status::Ok();
  }

  const std::string normalized_table = NormalizeIdentifier(statement.table_name);
  if (table_store_roots_.find(normalized_table) != table_store_roots_.end()) {
    return Status::Error("E4006: table-store root already exists for table '" + statement.table_name + "'");
  }

  storage::TableStore table_store(pager_.get());
  std::uint32_t root_page_id = 0U;
  const storage::TableStoreStatus init_status = table_store.Initialize(&root_page_id);
  if (!init_status.ok) {
    return Status::Error(init_status.code + ": " + init_status.message);
  }

  table_store_roots_.emplace(normalized_table, root_page_id);
  return Status::Ok("initialized table-store pages for new table");
}

Status DatabaseEngine::AppendInsertToTableStore(const parser::InsertStatement& statement) {
  if (!persistence_enabled_ || pager_ == nullptr) {
    return Status::Ok();
  }

  const std::string normalized_table = NormalizeIdentifier(statement.table_name);
  const auto root_iter = table_store_roots_.find(normalized_table);
  if (root_iter == table_store_roots_.end()) {
    return Status::Error("E4005: table-store root not found for table '" + statement.table_name + "'");
  }

  const catalog::SelectResult select_result = catalog_.SelectAll(parser::SelectStatement{statement.table_name});
  if (!select_result.status.ok) {
    return Status::Error(select_result.status.code + ": " + select_result.status.message);
  }

  std::vector<std::uint8_t> row_bytes;
  const storage::RowCodecStatus encode_status =
      storage::SerializeRow(select_result.columns, statement.values, &row_bytes);
  if (!encode_status.ok) {
    return Status::Error(encode_status.code + ": " + encode_status.message);
  }

  storage::TableStore table_store(pager_.get());
  storage::TableRowLocation location;
  const storage::TableStoreStatus append_status =
      table_store.AppendRow(root_iter->second, row_bytes, &location);
  if (!append_status.ok) {
    return Status::Error(append_status.code + ": " + append_status.message);
  }

  return Status::Ok("appended row into table-store page");
}

Status DatabaseEngine::Execute(std::string_view statement) {
  if (!startup_error_.empty()) {
    last_message_ = startup_error_;
    return Status::Error(last_message_);
  }

  const std::string trimmed = Trim(statement);

  if (trimmed.empty()) {
    last_message_ = "E0001: empty statement";
    return Status::Error(last_message_);
  }

  if (trimmed == ".help") {
    last_message_ = "meta commands: .help, .version, .exit";
    return Status::Ok(last_message_);
  }

  if (trimmed == ".version") {
    last_message_ = std::string(kEngineName) + " " + std::string(kVersion);
    return Status::Ok(last_message_);
  }

  if (trimmed.front() == '.') {
    last_message_ = "E0002: unknown meta command";
    return Status::Error(last_message_);
  }

  const parser::ParseResult parse_result = parser::ParseSql(trimmed);
  if (!parse_result.ok) {
    last_message_ = parse_result.error.code + ": " + parse_result.error.message;
    return Status::Error(last_message_);
  }

  if (std::holds_alternative<parser::CreateTableStatement>(parse_result.statement)) {
    const auto& create_statement = std::get<parser::CreateTableStatement>(parse_result.statement);
    const catalog::CatalogStatus create_status = catalog_.CreateTable(create_statement);
    if (!create_status.ok) {
      last_message_ = create_status.code + ": " + create_status.message;
      return Status::Error(last_message_);
    }

    const Status persist_status = PersistCatalogSnapshotToPager();
    if (!persist_status.ok) {
      last_message_ = persist_status.message;
      return Status::Error(last_message_);
    }

    const Status initialize_status = InitializeCreateTableStore(create_statement);
    if (!initialize_status.ok) {
      const Status rebuild_status = RebuildTableStoresFromCatalog();
      if (!rebuild_status.ok) {
        last_message_ = initialize_status.message;
        return Status::Error(last_message_);
      }
    }

    last_message_ = create_status.message;
    return Status::Ok(last_message_);
  }

  if (std::holds_alternative<parser::InsertStatement>(parse_result.statement)) {
    const auto& insert_statement = std::get<parser::InsertStatement>(parse_result.statement);
    const catalog::CatalogStatus insert_status = catalog_.InsertRow(insert_statement);
    if (!insert_status.ok) {
      last_message_ = insert_status.code + ": " + insert_status.message;
      return Status::Error(last_message_);
    }

    const Status persist_status = PersistCatalogSnapshotToPager();
    if (!persist_status.ok) {
      last_message_ = persist_status.message;
      return Status::Error(last_message_);
    }

    const Status append_status = AppendInsertToTableStore(insert_statement);
    if (!append_status.ok) {
      const Status rebuild_status = RebuildTableStoresFromCatalog();
      if (!rebuild_status.ok) {
        last_message_ = append_status.message;
        return Status::Error(last_message_);
      }
    }

    last_message_ = insert_status.message;
    return Status::Ok(last_message_);
  }

  if (std::holds_alternative<parser::SelectStatement>(parse_result.statement)) {
    const auto& select_statement = std::get<parser::SelectStatement>(parse_result.statement);
    const catalog::SelectResult select_result = catalog_.SelectAll(select_statement);
    if (!select_result.status.ok) {
      last_message_ = select_result.status.code + ": " + select_result.status.message;
      return Status::Error(last_message_);
    }

    std::vector<std::vector<parser::ValueLiteral>> result_rows = select_result.rows;

    if (persistence_enabled_ && pager_ != nullptr) {
      const std::string normalized_table = NormalizeIdentifier(select_statement.table_name);
      const auto root_iter = table_store_roots_.find(normalized_table);
      if (root_iter != table_store_roots_.end()) {
        storage::TableStore table_store(pager_.get());
        std::vector<storage::StoredTableRow> stored_rows;
        const storage::TableStoreStatus scan_status = table_store.ScanRows(root_iter->second, &stored_rows);
        if (!scan_status.ok) {
          last_message_ = scan_status.code + ": " + scan_status.message;
          return Status::Error(last_message_);
        }

        std::vector<std::vector<parser::ValueLiteral>> decoded_rows;
        decoded_rows.reserve(stored_rows.size());

        for (const storage::StoredTableRow& stored_row : stored_rows) {
          std::vector<parser::ValueLiteral> decoded_row;
          const storage::RowCodecStatus decode_status =
              storage::DeserializeRow(select_result.columns, stored_row.row_bytes, &decoded_row);
          if (!decode_status.ok) {
            last_message_ = decode_status.code + ": " + decode_status.message;
            return Status::Error(last_message_);
          }

          decoded_rows.push_back(std::move(decoded_row));
        }

        result_rows = std::move(decoded_rows);
      }
    }

    last_message_ =
        "selected " + std::to_string(static_cast<unsigned long long>(result_rows.size())) +
        " row(s) from '" + select_statement.table_name + "'";
    if (!result_rows.empty()) {
      last_message_ += ": " + FormatRows(result_rows);
    }

    return Status::Ok(last_message_);
  }

  if (std::holds_alternative<parser::UpdateStatement>(parse_result.statement)) {
    const auto& update_statement = std::get<parser::UpdateStatement>(parse_result.statement);
    const catalog::CatalogStatus update_status = catalog_.UpdateWhereEquals(update_statement);
    if (!update_status.ok) {
      last_message_ = update_status.code + ": " + update_status.message;
      return Status::Error(last_message_);
    }

    const Status persist_status = PersistCatalogSnapshotToPager();
    if (!persist_status.ok) {
      last_message_ = persist_status.message;
      return Status::Error(last_message_);
    }

    const Status rebuild_status = RebuildTableStoresFromCatalog();
    if (!rebuild_status.ok) {
      last_message_ = rebuild_status.message;
      return Status::Error(last_message_);
    }

    last_message_ = update_status.message;
    return Status::Ok(last_message_);
  }

  const auto& delete_statement = std::get<parser::DeleteStatement>(parse_result.statement);
  const catalog::CatalogStatus delete_status = catalog_.DeleteWhereEquals(delete_statement);
  if (!delete_status.ok) {
    last_message_ = delete_status.code + ": " + delete_status.message;
    return Status::Error(last_message_);
  }

  const Status persist_status = PersistCatalogSnapshotToPager();
  if (!persist_status.ok) {
    last_message_ = persist_status.message;
    return Status::Error(last_message_);
  }

  const Status rebuild_status = RebuildTableStoresFromCatalog();
  if (!rebuild_status.ok) {
    last_message_ = rebuild_status.message;
    return Status::Error(last_message_);
  }

  last_message_ = delete_status.message;

  return Status::Ok(last_message_);
}

std::string_view DatabaseEngine::LastMessage() const noexcept {
  return last_message_;
}

}  // namespace atlasdb
