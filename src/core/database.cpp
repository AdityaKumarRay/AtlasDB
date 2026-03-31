#include "atlasdb/database.hpp"

#include <cctype>
#include <string>
#include <utility>
#include <variant>

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

  const auto& insert_statement = std::get<parser::InsertStatement>(parse_result.statement);
  const catalog::CatalogStatus insert_status = catalog_.InsertRow(insert_statement);
  if (!insert_status.ok) {
    last_message_ = insert_status.code + ": " + insert_status.message;
    return Status::Error(last_message_);
  }

  last_message_ = insert_status.message;
  return Status::Ok(last_message_);
}

std::string_view DatabaseEngine::LastMessage() const noexcept {
  return last_message_;
}

}  // namespace atlasdb
