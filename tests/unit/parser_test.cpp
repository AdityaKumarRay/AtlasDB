#include "atlasdb/parser/ast.hpp"
#include "atlasdb/parser/parser.hpp"

#include <cstdint>
#include <string>
#include <variant>

#include <gtest/gtest.h>

namespace {

TEST(ParserCreateTable, ParsesCreateTableStatement) {
  const atlasdb::parser::ParseResult result =
      atlasdb::parser::ParseSql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);");

  ASSERT_TRUE(result.ok);
  ASSERT_TRUE(std::holds_alternative<atlasdb::parser::CreateTableStatement>(result.statement));

  const auto& create = std::get<atlasdb::parser::CreateTableStatement>(result.statement);
  EXPECT_EQ(create.table_name, "users");
  ASSERT_EQ(create.columns.size(), 2U);

  EXPECT_EQ(create.columns[0].name, "id");
  EXPECT_EQ(create.columns[0].type, atlasdb::parser::ColumnType::Integer);
  EXPECT_TRUE(create.columns[0].primary_key);

  EXPECT_EQ(create.columns[1].name, "name");
  EXPECT_EQ(create.columns[1].type, atlasdb::parser::ColumnType::Text);
  EXPECT_FALSE(create.columns[1].primary_key);
}

TEST(ParserInsert, ParsesInsertStatementWithLiterals) {
  const atlasdb::parser::ParseResult result =
      atlasdb::parser::ParseSql("INSERT INTO users VALUES (1, 'alice');");

  ASSERT_TRUE(result.ok);
  ASSERT_TRUE(std::holds_alternative<atlasdb::parser::InsertStatement>(result.statement));

  const auto& insert = std::get<atlasdb::parser::InsertStatement>(result.statement);
  EXPECT_EQ(insert.table_name, "users");
  ASSERT_EQ(insert.values.size(), 2U);
  EXPECT_EQ(std::get<std::int64_t>(insert.values[0].value), 1);
  EXPECT_EQ(std::get<std::string>(insert.values[1].value), "alice");
}

TEST(ParserErrors, RejectsUnsupportedStatement) {
  const atlasdb::parser::ParseResult result = atlasdb::parser::ParseSql("SELECT users;");

  ASSERT_FALSE(result.ok);
  EXPECT_EQ(result.error.code, "E1200");
  EXPECT_EQ(result.error.message, "unsupported statement; expected CREATE TABLE or INSERT INTO");
}

TEST(ParserErrors, RejectsInvalidColumnType) {
  const atlasdb::parser::ParseResult result = atlasdb::parser::ParseSql("CREATE TABLE t (id BLOB);");

  ASSERT_FALSE(result.ok);
  EXPECT_EQ(result.error.code, "E1206");
  EXPECT_EQ(result.error.message, "invalid column type 'BLOB'");
}

TEST(ParserErrors, RejectsUnterminatedString) {
  const atlasdb::parser::ParseResult result = atlasdb::parser::ParseSql("INSERT INTO t VALUES ('unterminated);");

  ASSERT_FALSE(result.ok);
  EXPECT_EQ(result.error.code, "E1101");
  EXPECT_EQ(result.error.message, "unterminated string literal");
}

}  // namespace
