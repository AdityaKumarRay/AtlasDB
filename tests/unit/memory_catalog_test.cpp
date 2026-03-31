#include "atlasdb/catalog/memory_catalog.hpp"

#include <cstdint>
#include <string>

#include <gtest/gtest.h>

namespace {

atlasdb::parser::CreateTableStatement UsersTableStatement() {
  return atlasdb::parser::CreateTableStatement{
      "users",
      {
          {"id", atlasdb::parser::ColumnType::Integer, true},
          {"name", atlasdb::parser::ColumnType::Text, false},
      },
  };
}

atlasdb::parser::InsertStatement UserInsertStatement(std::int64_t id, std::string name) {
  atlasdb::parser::InsertStatement statement;
  statement.table_name = "users";
  statement.values.push_back(atlasdb::parser::ValueLiteral{id});
  statement.values.push_back(atlasdb::parser::ValueLiteral{std::move(name)});
  return statement;
}

TEST(MemoryCatalog, CreatesTableAndInsertsRows) {
  atlasdb::catalog::MemoryCatalog catalog;

  const atlasdb::catalog::CatalogStatus create = catalog.CreateTable(UsersTableStatement());
  ASSERT_TRUE(create.ok);
  EXPECT_EQ(create.message, "created table 'users'");

  const atlasdb::catalog::CatalogStatus insert = catalog.InsertRow(UserInsertStatement(1, "alice"));
  ASSERT_TRUE(insert.ok);
  EXPECT_EQ(insert.message, "inserted 1 row into 'users'");
  EXPECT_EQ(catalog.RowCount("users"), 1U);
}

TEST(MemoryCatalog, RejectsDuplicateTableNamesIgnoringCase) {
  atlasdb::catalog::MemoryCatalog catalog;

  const atlasdb::catalog::CatalogStatus first = catalog.CreateTable(UsersTableStatement());
  ASSERT_TRUE(first.ok);

  atlasdb::parser::CreateTableStatement duplicate = UsersTableStatement();
  duplicate.table_name = "Users";

  const atlasdb::catalog::CatalogStatus second = catalog.CreateTable(duplicate);
  ASSERT_FALSE(second.ok);
  EXPECT_EQ(second.code, "E2001");
  EXPECT_EQ(second.message, "table already exists: Users");
}

TEST(MemoryCatalog, RejectsValueCountMismatch) {
  atlasdb::catalog::MemoryCatalog catalog;
  ASSERT_TRUE(catalog.CreateTable(UsersTableStatement()).ok);

  atlasdb::parser::InsertStatement statement;
  statement.table_name = "users";
  statement.values.push_back(atlasdb::parser::ValueLiteral{1LL});

  const atlasdb::catalog::CatalogStatus result = catalog.InsertRow(statement);
  ASSERT_FALSE(result.ok);
  EXPECT_EQ(result.code, "E2004");
  EXPECT_EQ(result.message, "value count mismatch: expected 2 but got 1");
}

TEST(MemoryCatalog, RejectsTypeMismatch) {
  atlasdb::catalog::MemoryCatalog catalog;
  ASSERT_TRUE(catalog.CreateTable(UsersTableStatement()).ok);

  atlasdb::parser::InsertStatement statement;
  statement.table_name = "users";
  statement.values.push_back(atlasdb::parser::ValueLiteral{std::string{"not-int"}});
  statement.values.push_back(atlasdb::parser::ValueLiteral{std::string{"alice"}});

  const atlasdb::catalog::CatalogStatus result = catalog.InsertRow(statement);
  ASSERT_FALSE(result.ok);
  EXPECT_EQ(result.code, "E2005");
  EXPECT_EQ(result.message, "type mismatch at column 'id': expected INTEGER");
}

TEST(MemoryCatalog, RejectsDuplicatePrimaryKey) {
  atlasdb::catalog::MemoryCatalog catalog;
  ASSERT_TRUE(catalog.CreateTable(UsersTableStatement()).ok);
  ASSERT_TRUE(catalog.InsertRow(UserInsertStatement(1, "alice")).ok);

  const atlasdb::catalog::CatalogStatus duplicate = catalog.InsertRow(UserInsertStatement(1, "bob"));
  ASSERT_FALSE(duplicate.ok);
  EXPECT_EQ(duplicate.code, "E2006");
  EXPECT_EQ(duplicate.message, "duplicate primary key for table 'users'");
}

TEST(MemoryCatalog, RejectsInsertToUnknownTable) {
  atlasdb::catalog::MemoryCatalog catalog;

  const atlasdb::catalog::CatalogStatus result = catalog.InsertRow(UserInsertStatement(1, "alice"));
  ASSERT_FALSE(result.ok);
  EXPECT_EQ(result.code, "E2003");
  EXPECT_EQ(result.message, "table not found: users");
}

}  // namespace
