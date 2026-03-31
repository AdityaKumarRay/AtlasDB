#pragma once

#include <cstddef>
#include <string>
#include <string_view>

#include "atlasdb/parser/ast.hpp"

namespace atlasdb::parser {

struct ParseError {
  std::string code{};
  std::string message{};
  std::size_t position{0};
};

struct ParseResult {
  bool ok{false};
  Statement statement{};
  ParseError error{};
};

[[nodiscard]] ParseResult ParseSql(std::string_view sql);

[[nodiscard]] std::string StatementTypeName(const Statement& statement);

}  // namespace atlasdb::parser
