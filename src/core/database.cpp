#include "atlasdb/database.hpp"

#include <cctype>
#include <cstdint>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "atlasdb/parser/ast.hpp"
#include "atlasdb/parser/parser.hpp"
#include "atlasdb/version.hpp"

namespace atlasdb {
namespace {

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

}  // namespace

Status Status::Ok(std::string message) {
  return Status{true, std::move(message)};
}

Status Status::Error(std::string message) {
  return Status{false, std::move(message)};
}

Status DatabaseEngine::Execute(std::string_view statement) {
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

    last_message_ = select_result.status.message;
    if (!select_result.rows.empty()) {
      last_message_ += ": " + FormatRows(select_result.rows);
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

    last_message_ = update_status.message;
    return Status::Ok(last_message_);
  }

  const auto& delete_statement = std::get<parser::DeleteStatement>(parse_result.statement);
  const catalog::CatalogStatus delete_status = catalog_.DeleteWhereEquals(delete_statement);
  if (!delete_status.ok) {
    last_message_ = delete_status.code + ": " + delete_status.message;
    return Status::Error(last_message_);
  }

  last_message_ = delete_status.message;

  return Status::Ok(last_message_);
}

std::string_view DatabaseEngine::LastMessage() const noexcept {
  return last_message_;
}

}  // namespace atlasdb
