#include "atlasdb/catalog/memory_catalog.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

namespace atlasdb::catalog {
namespace {

constexpr std::size_t kNoPrimaryKey = std::numeric_limits<std::size_t>::max();
constexpr std::size_t kColumnNotFound = std::numeric_limits<std::size_t>::max();
constexpr std::uint8_t kLiteralTagInteger = 1U;
constexpr std::uint8_t kLiteralTagText = 2U;
constexpr std::uint32_t kCatalogSnapshotVersion = 2U;
constexpr std::array<std::uint8_t, 8> kCatalogSnapshotMagic = {
    static_cast<std::uint8_t>('A'),
    static_cast<std::uint8_t>('T'),
    static_cast<std::uint8_t>('L'),
    static_cast<std::uint8_t>('C'),
    static_cast<std::uint8_t>('A'),
    static_cast<std::uint8_t>('T'),
    static_cast<std::uint8_t>('1'),
    0U,
};

void AppendBytes(std::vector<std::uint8_t>* out_bytes, const std::uint8_t* input, std::size_t count) {
  out_bytes->insert(out_bytes->end(), input, input + count);
}

void WriteUint8(std::vector<std::uint8_t>* out_bytes, std::uint8_t value) {
  out_bytes->push_back(value);
}

void WriteUint16(std::vector<std::uint8_t>* out_bytes, std::uint16_t value) {
  out_bytes->push_back(static_cast<std::uint8_t>(value & 0xFFU));
  out_bytes->push_back(static_cast<std::uint8_t>((value >> 8U) & 0xFFU));
}

void WriteUint32(std::vector<std::uint8_t>* out_bytes, std::uint32_t value) {
  out_bytes->push_back(static_cast<std::uint8_t>(value & 0xFFU));
  out_bytes->push_back(static_cast<std::uint8_t>((value >> 8U) & 0xFFU));
  out_bytes->push_back(static_cast<std::uint8_t>((value >> 16U) & 0xFFU));
  out_bytes->push_back(static_cast<std::uint8_t>((value >> 24U) & 0xFFU));
}

void WriteUint64(std::vector<std::uint8_t>* out_bytes, std::uint64_t value) {
  out_bytes->push_back(static_cast<std::uint8_t>(value & 0xFFU));
  out_bytes->push_back(static_cast<std::uint8_t>((value >> 8U) & 0xFFU));
  out_bytes->push_back(static_cast<std::uint8_t>((value >> 16U) & 0xFFU));
  out_bytes->push_back(static_cast<std::uint8_t>((value >> 24U) & 0xFFU));
  out_bytes->push_back(static_cast<std::uint8_t>((value >> 32U) & 0xFFU));
  out_bytes->push_back(static_cast<std::uint8_t>((value >> 40U) & 0xFFU));
  out_bytes->push_back(static_cast<std::uint8_t>((value >> 48U) & 0xFFU));
  out_bytes->push_back(static_cast<std::uint8_t>((value >> 56U) & 0xFFU));
}

bool WriteSizedString16(std::vector<std::uint8_t>* out_bytes, std::string_view value) {
  if (value.size() > static_cast<std::size_t>(std::numeric_limits<std::uint16_t>::max())) {
    return false;
  }

  WriteUint16(out_bytes, static_cast<std::uint16_t>(value.size()));
  AppendBytes(out_bytes, reinterpret_cast<const std::uint8_t*>(value.data()), value.size());
  return true;
}

bool WriteSizedString32(std::vector<std::uint8_t>* out_bytes, std::string_view value) {
  if (value.size() > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
    return false;
  }

  WriteUint32(out_bytes, static_cast<std::uint32_t>(value.size()));
  AppendBytes(out_bytes, reinterpret_cast<const std::uint8_t*>(value.data()), value.size());
  return true;
}

bool ReadUint8(const std::vector<std::uint8_t>& bytes, std::size_t* offset, std::uint8_t* value) {
  if (offset == nullptr || value == nullptr) {
    return false;
  }

  if (*offset + 1U > bytes.size()) {
    return false;
  }

  *value = bytes[*offset];
  *offset += 1U;
  return true;
}

bool ReadUint16(const std::vector<std::uint8_t>& bytes, std::size_t* offset, std::uint16_t* value) {
  if (offset == nullptr || value == nullptr) {
    return false;
  }

  if (*offset + 2U > bytes.size()) {
    return false;
  }

  const std::uint16_t b0 = static_cast<std::uint16_t>(bytes[*offset + 0U]);
  const std::uint16_t b1 = static_cast<std::uint16_t>(bytes[*offset + 1U]) << 8U;

  *value = static_cast<std::uint16_t>(b0 | b1);
  *offset += 2U;
  return true;
}

bool ReadUint32(const std::vector<std::uint8_t>& bytes, std::size_t* offset, std::uint32_t* value) {
  if (offset == nullptr || value == nullptr) {
    return false;
  }

  if (*offset + 4U > bytes.size()) {
    return false;
  }

  const std::uint32_t b0 = static_cast<std::uint32_t>(bytes[*offset + 0U]);
  const std::uint32_t b1 = static_cast<std::uint32_t>(bytes[*offset + 1U]) << 8U;
  const std::uint32_t b2 = static_cast<std::uint32_t>(bytes[*offset + 2U]) << 16U;
  const std::uint32_t b3 = static_cast<std::uint32_t>(bytes[*offset + 3U]) << 24U;

  *value = b0 | b1 | b2 | b3;
  *offset += 4U;
  return true;
}

bool ReadUint64(const std::vector<std::uint8_t>& bytes, std::size_t* offset, std::uint64_t* value) {
  if (offset == nullptr || value == nullptr) {
    return false;
  }

  if (*offset + 8U > bytes.size()) {
    return false;
  }

  const std::uint64_t b0 = static_cast<std::uint64_t>(bytes[*offset + 0U]);
  const std::uint64_t b1 = static_cast<std::uint64_t>(bytes[*offset + 1U]) << 8U;
  const std::uint64_t b2 = static_cast<std::uint64_t>(bytes[*offset + 2U]) << 16U;
  const std::uint64_t b3 = static_cast<std::uint64_t>(bytes[*offset + 3U]) << 24U;
  const std::uint64_t b4 = static_cast<std::uint64_t>(bytes[*offset + 4U]) << 32U;
  const std::uint64_t b5 = static_cast<std::uint64_t>(bytes[*offset + 5U]) << 40U;
  const std::uint64_t b6 = static_cast<std::uint64_t>(bytes[*offset + 6U]) << 48U;
  const std::uint64_t b7 = static_cast<std::uint64_t>(bytes[*offset + 7U]) << 56U;

  *value = b0 | b1 | b2 | b3 | b4 | b5 | b6 | b7;
  *offset += 8U;
  return true;
}

bool ReadSizedString16(const std::vector<std::uint8_t>& bytes, std::size_t* offset, std::string* value) {
  if (offset == nullptr || value == nullptr) {
    return false;
  }

  std::uint16_t length = 0U;
  if (!ReadUint16(bytes, offset, &length)) {
    return false;
  }

  if (*offset + static_cast<std::size_t>(length) > bytes.size()) {
    return false;
  }

  value->assign(reinterpret_cast<const char*>(bytes.data() + *offset), static_cast<std::size_t>(length));
  *offset += static_cast<std::size_t>(length);
  return true;
}

bool ReadSizedString32(const std::vector<std::uint8_t>& bytes, std::size_t* offset, std::string* value) {
  if (offset == nullptr || value == nullptr) {
    return false;
  }

  std::uint32_t length = 0U;
  if (!ReadUint32(bytes, offset, &length)) {
    return false;
  }

  if (*offset + static_cast<std::size_t>(length) > bytes.size()) {
    return false;
  }

  value->assign(reinterpret_cast<const char*>(bytes.data() + *offset), static_cast<std::size_t>(length));
  *offset += static_cast<std::size_t>(length);
  return true;
}

}  // namespace

CatalogStatus CatalogStatus::Ok(std::string message) {
  return CatalogStatus{true, "", std::move(message)};
}

CatalogStatus CatalogStatus::Error(std::string code, std::string message) {
  return CatalogStatus{false, std::move(code), std::move(message)};
}

CatalogStatus MemoryCatalog::CreateTable(const parser::CreateTableStatement& statement) {
  const std::string normalized_table_name = NormalizeIdentifier(statement.table_name);

  if (tables_.contains(normalized_table_name)) {
    return CatalogStatus::Error("E2001", "table already exists: " + statement.table_name);
  }

  std::unordered_set<std::string> column_names;
  std::size_t primary_key_index = kNoPrimaryKey;

  for (std::size_t index = 0; index < statement.columns.size(); ++index) {
    const parser::ColumnDefinition& column = statement.columns[index];
    const std::string normalized_column_name = NormalizeIdentifier(column.name);

    if (!column_names.insert(normalized_column_name).second) {
      return CatalogStatus::Error("E2002", "duplicate column name: " + column.name);
    }

    if (column.primary_key) {
      if (primary_key_index != kNoPrimaryKey) {
        return CatalogStatus::Error("E2010", "multiple PRIMARY KEY columns are not supported");
      }
      primary_key_index = index;
    }
  }

  Table table;
  table.name = statement.table_name;
  table.columns = statement.columns;
  table.primary_key_index = primary_key_index;

  tables_.emplace(normalized_table_name, std::move(table));
  return CatalogStatus::Ok("created table '" + statement.table_name + "'");
}

CatalogStatus MemoryCatalog::InsertRow(const parser::InsertStatement& statement) {
  const std::string normalized_table_name = NormalizeIdentifier(statement.table_name);
  auto table_iter = tables_.find(normalized_table_name);
  if (table_iter == tables_.end()) {
    return CatalogStatus::Error("E2003", "table not found: " + statement.table_name);
  }

  Table& table = table_iter->second;

  if (statement.values.size() != table.columns.size()) {
    return CatalogStatus::Error(
        "E2004",
        "value count mismatch: expected " +
            std::to_string(static_cast<unsigned long long>(table.columns.size())) + " but got " +
            std::to_string(static_cast<unsigned long long>(statement.values.size())));
  }

  for (std::size_t index = 0; index < table.columns.size(); ++index) {
    const parser::ColumnDefinition& column = table.columns[index];
    const parser::ValueLiteral& value = statement.values[index];

    if (!ValueMatchesType(value, column.type)) {
      return CatalogStatus::Error("E2005", "type mismatch at column '" + column.name + "': expected " +
                                               TypeName(column.type));
    }
  }

  if (table.primary_key_index != kNoPrimaryKey) {
    const std::string token = PrimaryKeyToken(statement.values[table.primary_key_index]);
    if (table.primary_key_values.contains(token)) {
      return CatalogStatus::Error("E2006", "duplicate primary key for table '" + table.name + "'");
    }

    table.primary_key_values.insert(token);
  }

  table.rows.push_back(statement.values);
  return CatalogStatus::Ok("inserted 1 row into '" + table.name + "'");
}

SelectResult MemoryCatalog::SelectAll(const parser::SelectStatement& statement) const {
  SelectResult result;

  const std::string normalized_table_name = NormalizeIdentifier(statement.table_name);
  const auto table_iter = tables_.find(normalized_table_name);
  if (table_iter == tables_.end()) {
    result.status = CatalogStatus::Error("E2003", "table not found: " + statement.table_name);
    return result;
  }

  const Table& table = table_iter->second;
  result.status = CatalogStatus::Ok(
      "selected " + std::to_string(static_cast<unsigned long long>(table.rows.size())) +
      " row(s) from '" + table.name + "'");
  result.columns = table.columns;
  result.rows = table.rows;
  return result;
}

CatalogStatus MemoryCatalog::CreateSecondaryIndex(std::string_view table_name,
                                                  std::string_view index_name,
                                                  std::string_view column_name) {
  const std::string normalized_table_name = NormalizeIdentifier(table_name);
  auto table_iter = tables_.find(normalized_table_name);
  if (table_iter == tables_.end()) {
    return CatalogStatus::Error("E2003", "table not found: " + std::string(table_name));
  }

  const std::string normalized_index_name = NormalizeIdentifier(index_name);
  if (normalized_index_name.empty()) {
    return CatalogStatus::Error("E2011", "secondary index name is empty");
  }

  Table& table = table_iter->second;
  if (table.secondary_index_names.contains(normalized_index_name)) {
    return CatalogStatus::Error("E2013", "secondary index already exists: " + std::string(index_name));
  }

  const std::size_t column_index = FindColumnIndex(table, column_name);
  if (column_index == kColumnNotFound) {
    return CatalogStatus::Error("E2012", "secondary index column not found: " + std::string(column_name));
  }

  const std::string normalized_column_name = NormalizeIdentifier(table.columns[column_index].name);
  if (table.secondary_indexed_columns.contains(normalized_column_name)) {
    return CatalogStatus::Error("E2014", "secondary index already exists on column: " +
                                             table.columns[column_index].name);
  }

  table.secondary_indexes.push_back(
      SecondaryIndexDefinition{std::string(index_name), table.columns[column_index].name});
  table.secondary_index_names.insert(normalized_index_name);
  table.secondary_indexed_columns.insert(normalized_column_name);

  return CatalogStatus::Ok("created secondary index '" + std::string(index_name) + "' on '" +
                           table.name + "." + table.columns[column_index].name + "'");
}

SecondaryIndexListResult MemoryCatalog::ListSecondaryIndexes(std::string_view table_name) const {
  SecondaryIndexListResult result;

  const std::string normalized_table_name = NormalizeIdentifier(table_name);
  const auto table_iter = tables_.find(normalized_table_name);
  if (table_iter == tables_.end()) {
    result.status = CatalogStatus::Error("E2003", "table not found: " + std::string(table_name));
    return result;
  }

  result.status = CatalogStatus::Ok("listed secondary indexes for table '" + table_iter->second.name + "'");
  result.indexes = table_iter->second.secondary_indexes;
  std::sort(result.indexes.begin(), result.indexes.end(),
            [](const SecondaryIndexDefinition& lhs, const SecondaryIndexDefinition& rhs) {
              return MemoryCatalog::NormalizeIdentifier(lhs.name) <
                     MemoryCatalog::NormalizeIdentifier(rhs.name);
            });
  return result;
}

CatalogStatus MemoryCatalog::UpdateWhereEquals(const parser::UpdateStatement& statement) {
  const std::string normalized_table_name = NormalizeIdentifier(statement.table_name);
  auto table_iter = tables_.find(normalized_table_name);
  if (table_iter == tables_.end()) {
    return CatalogStatus::Error("E2003", "table not found: " + statement.table_name);
  }

  Table& table = table_iter->second;

  const std::size_t assignment_column_index = FindColumnIndex(table, statement.assignment.column_name);
  if (assignment_column_index == kColumnNotFound) {
    return CatalogStatus::Error("E2007", "unknown column: " + statement.assignment.column_name);
  }

  const std::size_t predicate_column_index = FindColumnIndex(table, statement.predicate.column_name);
  if (predicate_column_index == kColumnNotFound) {
    return CatalogStatus::Error("E2007", "unknown column: " + statement.predicate.column_name);
  }

  if (predicate_column_index != table.primary_key_index) {
    return CatalogStatus::Error("E2008", "WHERE column must be PRIMARY KEY: " + statement.predicate.column_name);
  }

  const parser::ColumnDefinition& assignment_column = table.columns[assignment_column_index];
  const parser::ColumnDefinition& predicate_column = table.columns[predicate_column_index];

  if (!ValueMatchesType(statement.assignment.value, assignment_column.type)) {
    return CatalogStatus::Error("E2005", "type mismatch at column '" + assignment_column.name + "': expected " +
                                             TypeName(assignment_column.type));
  }

  if (!ValueMatchesType(statement.predicate.value, predicate_column.type)) {
    return CatalogStatus::Error("E2005", "type mismatch at column '" + predicate_column.name + "': expected " +
                                             TypeName(predicate_column.type));
  }

  std::size_t matched_row_index = kColumnNotFound;
  for (std::size_t row_index = 0; row_index < table.rows.size(); ++row_index) {
    if (LiteralEquals(table.rows[row_index][predicate_column_index], statement.predicate.value)) {
      matched_row_index = row_index;
      break;
    }
  }

  if (matched_row_index == kColumnNotFound) {
    return CatalogStatus::Error("E2009", "row not found for key match in table '" + table.name + "'");
  }

  if (assignment_column_index == table.primary_key_index) {
    const std::string current_key = PrimaryKeyToken(table.rows[matched_row_index][table.primary_key_index]);
    const std::string replacement_key = PrimaryKeyToken(statement.assignment.value);
    if (replacement_key != current_key && table.primary_key_values.contains(replacement_key)) {
      return CatalogStatus::Error("E2006", "duplicate primary key for table '" + table.name + "'");
    }

    if (replacement_key != current_key) {
      table.primary_key_values.erase(current_key);
      table.primary_key_values.insert(replacement_key);
    }
  }

  table.rows[matched_row_index][assignment_column_index] = statement.assignment.value;
  return CatalogStatus::Ok("updated 1 row in '" + table.name + "'");
}

CatalogStatus MemoryCatalog::DeleteWhereEquals(const parser::DeleteStatement& statement) {
  const std::string normalized_table_name = NormalizeIdentifier(statement.table_name);
  auto table_iter = tables_.find(normalized_table_name);
  if (table_iter == tables_.end()) {
    return CatalogStatus::Error("E2003", "table not found: " + statement.table_name);
  }

  Table& table = table_iter->second;

  const std::size_t predicate_column_index = FindColumnIndex(table, statement.predicate.column_name);
  if (predicate_column_index == kColumnNotFound) {
    return CatalogStatus::Error("E2007", "unknown column: " + statement.predicate.column_name);
  }

  if (predicate_column_index != table.primary_key_index) {
    return CatalogStatus::Error("E2008", "WHERE column must be PRIMARY KEY: " + statement.predicate.column_name);
  }

  const parser::ColumnDefinition& predicate_column = table.columns[predicate_column_index];
  if (!ValueMatchesType(statement.predicate.value, predicate_column.type)) {
    return CatalogStatus::Error("E2005", "type mismatch at column '" + predicate_column.name + "': expected " +
                                             TypeName(predicate_column.type));
  }

  std::size_t matched_row_index = kColumnNotFound;
  for (std::size_t row_index = 0; row_index < table.rows.size(); ++row_index) {
    if (LiteralEquals(table.rows[row_index][predicate_column_index], statement.predicate.value)) {
      matched_row_index = row_index;
      break;
    }
  }

  if (matched_row_index == kColumnNotFound) {
    return CatalogStatus::Error("E2009", "row not found for key match in table '" + table.name + "'");
  }

  if (table.primary_key_index != kNoPrimaryKey) {
    const std::string existing_key = PrimaryKeyToken(table.rows[matched_row_index][table.primary_key_index]);
    table.primary_key_values.erase(existing_key);
  }

  table.rows.erase(table.rows.begin() + static_cast<std::ptrdiff_t>(matched_row_index));
  return CatalogStatus::Ok("deleted 1 row from '" + table.name + "'");
}

CatalogStatus MemoryCatalog::Serialize(std::vector<std::uint8_t>* out_bytes) const {
  if (out_bytes == nullptr) {
    return CatalogStatus::Error("E2020", "output snapshot pointer is null");
  }

  if (tables_.size() > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
    return CatalogStatus::Error("E2020", "table count exceeds snapshot format limit");
  }

  std::vector<std::uint8_t> bytes;
  bytes.reserve(128U);
  AppendBytes(&bytes, kCatalogSnapshotMagic.data(), kCatalogSnapshotMagic.size());
  WriteUint32(&bytes, kCatalogSnapshotVersion);
  WriteUint32(&bytes, static_cast<std::uint32_t>(tables_.size()));

  std::vector<std::string> table_keys;
  table_keys.reserve(tables_.size());
  for (const auto& entry : tables_) {
    table_keys.push_back(entry.first);
  }
  std::sort(table_keys.begin(), table_keys.end());

  for (const std::string& table_key : table_keys) {
    const Table& table = tables_.at(table_key);

    if (!WriteSizedString16(&bytes, table.name)) {
      return CatalogStatus::Error("E2020", "table name exceeds snapshot format limit");
    }

    if (table.columns.size() > static_cast<std::size_t>(std::numeric_limits<std::uint16_t>::max())) {
      return CatalogStatus::Error("E2020", "column count exceeds snapshot format limit");
    }

    if (table.rows.size() > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
      return CatalogStatus::Error("E2020", "row count exceeds snapshot format limit");
    }

    if (table.secondary_indexes.size() > static_cast<std::size_t>(std::numeric_limits<std::uint16_t>::max())) {
      return CatalogStatus::Error("E2020", "secondary index count exceeds snapshot format limit");
    }

    WriteUint16(&bytes, static_cast<std::uint16_t>(table.columns.size()));
    WriteUint32(&bytes, static_cast<std::uint32_t>(table.rows.size()));

    for (const parser::ColumnDefinition& column : table.columns) {
      if (!WriteSizedString16(&bytes, column.name)) {
        return CatalogStatus::Error("E2020", "column name exceeds snapshot format limit");
      }

      WriteUint8(&bytes, static_cast<std::uint8_t>(column.type));
      WriteUint8(&bytes, static_cast<std::uint8_t>(column.primary_key ? 1U : 0U));
    }

    std::vector<SecondaryIndexDefinition> secondary_indexes = table.secondary_indexes;
    std::sort(secondary_indexes.begin(), secondary_indexes.end(),
              [](const SecondaryIndexDefinition& lhs, const SecondaryIndexDefinition& rhs) {
                return MemoryCatalog::NormalizeIdentifier(lhs.name) <
                       MemoryCatalog::NormalizeIdentifier(rhs.name);
              });

    WriteUint16(&bytes, static_cast<std::uint16_t>(secondary_indexes.size()));
    for (const SecondaryIndexDefinition& secondary_index : secondary_indexes) {
      if (!WriteSizedString16(&bytes, secondary_index.name)) {
        return CatalogStatus::Error("E2020", "secondary index name exceeds snapshot format limit");
      }

      if (!WriteSizedString16(&bytes, secondary_index.column_name)) {
        return CatalogStatus::Error("E2020", "secondary index column name exceeds snapshot format limit");
      }
    }

    for (const std::vector<parser::ValueLiteral>& row : table.rows) {
      if (row.size() != table.columns.size()) {
        return CatalogStatus::Error("E2020", "internal row width mismatch during snapshot serialization");
      }

      for (const parser::ValueLiteral& literal : row) {
        if (std::holds_alternative<std::int64_t>(literal.value)) {
          WriteUint8(&bytes, kLiteralTagInteger);
          WriteUint64(&bytes, static_cast<std::uint64_t>(std::get<std::int64_t>(literal.value)));
          continue;
        }

        WriteUint8(&bytes, kLiteralTagText);
        if (!WriteSizedString32(&bytes, std::get<std::string>(literal.value))) {
          return CatalogStatus::Error("E2020", "text literal exceeds snapshot format limit");
        }
      }
    }
  }

  *out_bytes = std::move(bytes);
  return CatalogStatus::Ok("serialized catalog snapshot");
}

CatalogStatus MemoryCatalog::Deserialize(const std::vector<std::uint8_t>& bytes) {
  tables_.clear();

  if (bytes.empty()) {
    return CatalogStatus::Ok("loaded empty catalog snapshot");
  }

  if (bytes.size() < kCatalogSnapshotMagic.size() + 8U) {
    return CatalogStatus::Error("E2021", "catalog snapshot is truncated");
  }

  std::size_t offset = 0U;
  for (std::size_t index = 0; index < kCatalogSnapshotMagic.size(); ++index) {
    if (bytes[index] != kCatalogSnapshotMagic[index]) {
      return CatalogStatus::Error("E2021", "invalid catalog snapshot magic");
    }
  }
  offset += kCatalogSnapshotMagic.size();

  std::uint32_t version = 0U;
  if (!ReadUint32(bytes, &offset, &version)) {
    return CatalogStatus::Error("E2021", "catalog snapshot is truncated");
  }

  if (version != 1U && version != kCatalogSnapshotVersion) {
    return CatalogStatus::Error("E2022", "unsupported catalog snapshot version");
  }

  std::uint32_t table_count = 0U;
  if (!ReadUint32(bytes, &offset, &table_count)) {
    return CatalogStatus::Error("E2021", "catalog snapshot is truncated");
  }

  for (std::uint32_t table_index = 0U; table_index < table_count; ++table_index) {
    std::string table_name;
    if (!ReadSizedString16(bytes, &offset, &table_name)) {
      return CatalogStatus::Error("E2021", "catalog snapshot table name is truncated");
    }

    std::uint16_t column_count = 0U;
    std::uint32_t row_count = 0U;
    if (!ReadUint16(bytes, &offset, &column_count) || !ReadUint32(bytes, &offset, &row_count)) {
      return CatalogStatus::Error("E2021", "catalog snapshot table metadata is truncated");
    }

    std::vector<parser::ColumnDefinition> columns;
    columns.reserve(static_cast<std::size_t>(column_count));

    for (std::uint16_t column_index = 0U; column_index < column_count; ++column_index) {
      std::string column_name;
      if (!ReadSizedString16(bytes, &offset, &column_name)) {
        return CatalogStatus::Error("E2021", "catalog snapshot column definition is truncated");
      }

      std::uint8_t type_byte = 0U;
      std::uint8_t primary_key_byte = 0U;
      if (!ReadUint8(bytes, &offset, &type_byte) || !ReadUint8(bytes, &offset, &primary_key_byte)) {
        return CatalogStatus::Error("E2021", "catalog snapshot column definition is truncated");
      }

      parser::ColumnType type = parser::ColumnType::Integer;
      if (type_byte == static_cast<std::uint8_t>(parser::ColumnType::Integer)) {
        type = parser::ColumnType::Integer;
      } else if (type_byte == static_cast<std::uint8_t>(parser::ColumnType::Text)) {
        type = parser::ColumnType::Text;
      } else {
        return CatalogStatus::Error("E2023", "catalog snapshot has unknown column type");
      }

      columns.push_back(parser::ColumnDefinition{column_name, type, primary_key_byte != 0U});
    }

    std::vector<SecondaryIndexDefinition> secondary_indexes;
    if (version >= 2U) {
      std::uint16_t secondary_index_count = 0U;
      if (!ReadUint16(bytes, &offset, &secondary_index_count)) {
        return CatalogStatus::Error("E2021", "catalog snapshot secondary index metadata is truncated");
      }

      secondary_indexes.reserve(static_cast<std::size_t>(secondary_index_count));
      for (std::uint16_t secondary_index_index = 0U;
           secondary_index_index < secondary_index_count;
           ++secondary_index_index) {
        std::string secondary_index_name;
        std::string secondary_index_column_name;
        if (!ReadSizedString16(bytes, &offset, &secondary_index_name) ||
            !ReadSizedString16(bytes, &offset, &secondary_index_column_name)) {
          return CatalogStatus::Error("E2021", "catalog snapshot secondary index definition is truncated");
        }

        secondary_indexes.push_back(
            SecondaryIndexDefinition{std::move(secondary_index_name),
                                     std::move(secondary_index_column_name)});
      }
    }

    const CatalogStatus create_status = CreateTable(parser::CreateTableStatement{table_name, columns});
    if (!create_status.ok) {
      return CatalogStatus::Error("E2024", "invalid table definition in catalog snapshot");
    }

    for (const SecondaryIndexDefinition& secondary_index : secondary_indexes) {
      const CatalogStatus index_status =
          CreateSecondaryIndex(table_name, secondary_index.name, secondary_index.column_name);
      if (!index_status.ok) {
        return CatalogStatus::Error("E2024", "invalid secondary index definition in catalog snapshot");
      }
    }

    for (std::uint32_t row_index = 0U; row_index < row_count; ++row_index) {
      parser::InsertStatement insert_statement;
      insert_statement.table_name = table_name;
      insert_statement.values.reserve(static_cast<std::size_t>(column_count));

      for (std::uint16_t column_index = 0U; column_index < column_count; ++column_index) {
        std::uint8_t tag = 0U;
        if (!ReadUint8(bytes, &offset, &tag)) {
          return CatalogStatus::Error("E2021", "catalog snapshot row payload is truncated");
        }

        if (tag == kLiteralTagInteger) {
          std::uint64_t encoded_value = 0U;
          if (!ReadUint64(bytes, &offset, &encoded_value)) {
            return CatalogStatus::Error("E2021", "catalog snapshot integer literal is truncated");
          }

          insert_statement.values.emplace_back(static_cast<std::int64_t>(encoded_value));
          continue;
        }

        if (tag == kLiteralTagText) {
          std::string text_value;
          if (!ReadSizedString32(bytes, &offset, &text_value)) {
            return CatalogStatus::Error("E2021", "catalog snapshot text literal is truncated");
          }

          insert_statement.values.emplace_back(std::move(text_value));
          continue;
        }

        return CatalogStatus::Error("E2025", "catalog snapshot has unknown literal tag");
      }

      const CatalogStatus insert_status = InsertRow(insert_statement);
      if (!insert_status.ok) {
        return CatalogStatus::Error("E2026", "invalid row data in catalog snapshot");
      }
    }
  }

  if (offset != bytes.size()) {
    return CatalogStatus::Error("E2027", "catalog snapshot contains trailing bytes");
  }

  return CatalogStatus::Ok("loaded catalog snapshot");
}

std::vector<TableSnapshot> MemoryCatalog::SnapshotTables() const {
  std::vector<TableSnapshot> snapshots;
  snapshots.reserve(tables_.size());

  std::vector<std::string> table_keys;
  table_keys.reserve(tables_.size());
  for (const auto& entry : tables_) {
    table_keys.push_back(entry.first);
  }
  std::sort(table_keys.begin(), table_keys.end());

  for (const std::string& table_key : table_keys) {
    const Table& table = tables_.at(table_key);

    TableSnapshot snapshot;
    snapshot.name = table.name;
    snapshot.columns = table.columns;
    snapshot.secondary_indexes = table.secondary_indexes;
    std::sort(snapshot.secondary_indexes.begin(), snapshot.secondary_indexes.end(),
              [](const SecondaryIndexDefinition& lhs, const SecondaryIndexDefinition& rhs) {
                return MemoryCatalog::NormalizeIdentifier(lhs.name) <
                       MemoryCatalog::NormalizeIdentifier(rhs.name);
              });
    snapshot.rows = table.rows;
    snapshots.push_back(std::move(snapshot));
  }

  return snapshots;
}

bool MemoryCatalog::HasTable(std::string_view table_name) const {
  return tables_.contains(NormalizeIdentifier(table_name));
}

std::size_t MemoryCatalog::RowCount(std::string_view table_name) const {
  const auto table_iter = tables_.find(NormalizeIdentifier(table_name));
  if (table_iter == tables_.end()) {
    return 0;
  }

  return table_iter->second.rows.size();
}

std::string MemoryCatalog::NormalizeIdentifier(std::string_view identifier) {
  std::string normalized(identifier);
  for (char& value : normalized) {
    value = static_cast<char>(std::tolower(static_cast<unsigned char>(value)));
  }
  return normalized;
}

std::size_t MemoryCatalog::FindColumnIndex(const Table& table, std::string_view column_name) {
  const std::string normalized_column_name = NormalizeIdentifier(column_name);
  for (std::size_t index = 0; index < table.columns.size(); ++index) {
    if (NormalizeIdentifier(table.columns[index].name) == normalized_column_name) {
      return index;
    }
  }

  return kColumnNotFound;
}

bool MemoryCatalog::LiteralEquals(const parser::ValueLiteral& lhs, const parser::ValueLiteral& rhs) {
  if (lhs.value.index() != rhs.value.index()) {
    return false;
  }

  if (std::holds_alternative<std::int64_t>(lhs.value)) {
    return std::get<std::int64_t>(lhs.value) == std::get<std::int64_t>(rhs.value);
  }

  return std::get<std::string>(lhs.value) == std::get<std::string>(rhs.value);
}

bool MemoryCatalog::ValueMatchesType(const parser::ValueLiteral& value, parser::ColumnType type) {
  switch (type) {
    case parser::ColumnType::Integer:
      return std::holds_alternative<std::int64_t>(value.value);
    case parser::ColumnType::Text:
      return std::holds_alternative<std::string>(value.value);
  }

  return false;
}

std::string MemoryCatalog::TypeName(parser::ColumnType type) {
  switch (type) {
    case parser::ColumnType::Integer:
      return "INTEGER";
    case parser::ColumnType::Text:
      return "TEXT";
  }

  return "UNKNOWN";
}

std::string MemoryCatalog::PrimaryKeyToken(const parser::ValueLiteral& value) {
  if (std::holds_alternative<std::int64_t>(value.value)) {
    return "I:" + std::to_string(std::get<std::int64_t>(value.value));
  }

  return "S:" + std::get<std::string>(value.value);
}

}  // namespace atlasdb::catalog
