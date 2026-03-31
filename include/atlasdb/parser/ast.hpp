#pragma once

#include <cstdint>
#include <string>
#include <variant>
#include <vector>

namespace atlasdb::parser {

enum class ColumnType {
  Integer,
  Text,
};

struct ColumnDefinition {
  std::string name;
  ColumnType type;
  bool primary_key;
};

struct CreateTableStatement {
  std::string table_name;
  std::vector<ColumnDefinition> columns;
};

struct ValueLiteral {
  std::variant<std::int64_t, std::string> value;
};

struct InsertStatement {
  std::string table_name;
  std::vector<ValueLiteral> values;
};

struct SelectStatement {
  std::string table_name;
};

struct Assignment {
  std::string column_name;
  ValueLiteral value;
};

struct EqualityPredicate {
  std::string column_name;
  ValueLiteral value;
};

struct UpdateStatement {
  std::string table_name;
  Assignment assignment;
  EqualityPredicate predicate;
};

struct DeleteStatement {
  std::string table_name;
  EqualityPredicate predicate;
};

using Statement =
    std::variant<CreateTableStatement, InsertStatement, SelectStatement, UpdateStatement, DeleteStatement>;

}  // namespace atlasdb::parser
