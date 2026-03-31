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

TEST(ParserSelect, ParsesSelectAllStatement) {
  const atlasdb::parser::ParseResult result = atlasdb::parser::ParseSql("SELECT * FROM users;");

  ASSERT_TRUE(result.ok);
  ASSERT_TRUE(std::holds_alternative<atlasdb::parser::SelectStatement>(result.statement));

  const auto& select = std::get<atlasdb::parser::SelectStatement>(result.statement);
  EXPECT_EQ(select.table_name, "users");
}

TEST(ParserUpdate, ParsesUpdateStatement) {
  const atlasdb::parser::ParseResult result =
      atlasdb::parser::ParseSql("UPDATE users SET name = 'bob' WHERE id = 1;");

  ASSERT_TRUE(result.ok);
  ASSERT_TRUE(std::holds_alternative<atlasdb::parser::UpdateStatement>(result.statement));

  const auto& update = std::get<atlasdb::parser::UpdateStatement>(result.statement);
  EXPECT_EQ(update.table_name, "users");
  EXPECT_EQ(update.assignment.column_name, "name");
  EXPECT_EQ(std::get<std::string>(update.assignment.value.value), "bob");
  EXPECT_EQ(update.predicate.column_name, "id");
  EXPECT_EQ(std::get<std::int64_t>(update.predicate.value.value), 1);
}

TEST(ParserDelete, ParsesDeleteStatement) {
  const atlasdb::parser::ParseResult result =
      atlasdb::parser::ParseSql("DELETE FROM users WHERE id = 1;");

  ASSERT_TRUE(result.ok);
  ASSERT_TRUE(std::holds_alternative<atlasdb::parser::DeleteStatement>(result.statement));

  const auto& deletion = std::get<atlasdb::parser::DeleteStatement>(result.statement);
  EXPECT_EQ(deletion.table_name, "users");
  EXPECT_EQ(deletion.predicate.column_name, "id");
  EXPECT_EQ(std::get<std::int64_t>(deletion.predicate.value.value), 1);
}

TEST(ParserErrors, RejectsUnsupportedStatement) {
  const atlasdb::parser::ParseResult result = atlasdb::parser::ParseSql("DROP TABLE users;");

  ASSERT_FALSE(result.ok);
  EXPECT_EQ(result.error.code, "E1200");
  EXPECT_EQ(result.error.message,
            "unsupported statement; expected CREATE TABLE, INSERT INTO, SELECT * FROM, UPDATE, or DELETE FROM");
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

TEST(ParserErrors, RejectsMalformedSelectWithoutStar) {
  const atlasdb::parser::ParseResult result = atlasdb::parser::ParseSql("SELECT id FROM users;");

  ASSERT_FALSE(result.ok);
  EXPECT_EQ(result.error.code, "E1401");
  EXPECT_EQ(result.error.message, "expected '*' after SELECT");
}

TEST(ParserErrors, RejectsMalformedUpdateWithoutWhere) {
  const atlasdb::parser::ParseResult result = atlasdb::parser::ParseSql("UPDATE users SET name = 'bob';");

  ASSERT_FALSE(result.ok);
  EXPECT_EQ(result.error.code, "E1506");
  EXPECT_EQ(result.error.message, "expected WHERE keyword after assignment");
}

TEST(ParserErrors, RejectsMalformedDeleteWithoutWhere) {
  const atlasdb::parser::ParseResult result = atlasdb::parser::ParseSql("DELETE FROM users;");

  ASSERT_FALSE(result.ok);
  EXPECT_EQ(result.error.code, "E1603");
  EXPECT_EQ(result.error.message, "expected WHERE keyword after table name");
}

}  // namespace
