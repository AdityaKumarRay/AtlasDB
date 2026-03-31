#include "atlasdb/catalog/memory_catalog.hpp"

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
