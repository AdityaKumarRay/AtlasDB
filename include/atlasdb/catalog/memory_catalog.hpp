#pragma once

#include <cstdint>
#include <cstddef>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "atlasdb/parser/ast.hpp"

namespace atlasdb::catalog {

struct CatalogStatus {
  bool ok;
  std::string code;
  std::string message;

  static CatalogStatus Ok(std::string message = {});
  static CatalogStatus Error(std::string code, std::string message);
};

struct SelectResult {
  CatalogStatus status;
  std::vector<parser::ColumnDefinition> columns;
  std::vector<std::vector<parser::ValueLiteral>> rows;
};

class MemoryCatalog {
 public:
  MemoryCatalog() = default;

  [[nodiscard]] CatalogStatus CreateTable(const parser::CreateTableStatement& statement);
  [[nodiscard]] CatalogStatus InsertRow(const parser::InsertStatement& statement);
  [[nodiscard]] SelectResult SelectAll(const parser::SelectStatement& statement) const;
  [[nodiscard]] CatalogStatus UpdateWhereEquals(const parser::UpdateStatement& statement);
  [[nodiscard]] CatalogStatus DeleteWhereEquals(const parser::DeleteStatement& statement);
  [[nodiscard]] CatalogStatus Serialize(std::vector<std::uint8_t>* out_bytes) const;
  [[nodiscard]] CatalogStatus Deserialize(const std::vector<std::uint8_t>& bytes);

  [[nodiscard]] bool HasTable(std::string_view table_name) const;
  [[nodiscard]] std::size_t RowCount(std::string_view table_name) const;

 private:
  struct Table {
    std::string name;
    std::vector<parser::ColumnDefinition> columns;
    std::size_t primary_key_index;
    std::vector<std::vector<parser::ValueLiteral>> rows;
    std::unordered_set<std::string> primary_key_values;
  };

  [[nodiscard]] static std::string NormalizeIdentifier(std::string_view identifier);
  [[nodiscard]] static std::size_t FindColumnIndex(const Table& table, std::string_view column_name);
  [[nodiscard]] static bool LiteralEquals(const parser::ValueLiteral& lhs, const parser::ValueLiteral& rhs);
  [[nodiscard]] static bool ValueMatchesType(const parser::ValueLiteral& value, parser::ColumnType type);
  [[nodiscard]] static std::string TypeName(parser::ColumnType type);
  [[nodiscard]] static std::string PrimaryKeyToken(const parser::ValueLiteral& value);

  std::unordered_map<std::string, Table> tables_;
};

}  // namespace atlasdb::catalog
