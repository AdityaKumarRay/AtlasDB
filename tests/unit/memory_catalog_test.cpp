#include "atlasdb/catalog/memory_catalog.hpp"

#include <cstdint>
#include <string>
#include <variant>

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

TEST(MemoryCatalog, SelectsRowsInInsertOrder) {
  atlasdb::catalog::MemoryCatalog catalog;
  ASSERT_TRUE(catalog.CreateTable(UsersTableStatement()).ok);
  ASSERT_TRUE(catalog.InsertRow(UserInsertStatement(1, "alice")).ok);
  ASSERT_TRUE(catalog.InsertRow(UserInsertStatement(2, "bob")).ok);

  atlasdb::parser::SelectStatement statement{"users"};
  const atlasdb::catalog::SelectResult result = catalog.SelectAll(statement);

  ASSERT_TRUE(result.status.ok);
  EXPECT_EQ(result.status.message, "selected 2 row(s) from 'users'");
  ASSERT_EQ(result.rows.size(), 2U);
  ASSERT_EQ(result.rows[0].size(), 2U);
  ASSERT_EQ(result.rows[1].size(), 2U);

  EXPECT_EQ(std::get<std::int64_t>(result.rows[0][0].value), 1);
  EXPECT_EQ(std::get<std::string>(result.rows[0][1].value), "alice");
  EXPECT_EQ(std::get<std::int64_t>(result.rows[1][0].value), 2);
  EXPECT_EQ(std::get<std::string>(result.rows[1][1].value), "bob");
}

TEST(MemoryCatalog, RejectsSelectFromUnknownTable) {
  atlasdb::catalog::MemoryCatalog catalog;

  const atlasdb::catalog::SelectResult result = catalog.SelectAll(atlasdb::parser::SelectStatement{"users"});
  ASSERT_FALSE(result.status.ok);
  EXPECT_EQ(result.status.code, "E2003");
  EXPECT_EQ(result.status.message, "table not found: users");
}

TEST(MemoryCatalog, UpdatesRowByPrimaryKey) {
  atlasdb::catalog::MemoryCatalog catalog;
  ASSERT_TRUE(catalog.CreateTable(UsersTableStatement()).ok);
  ASSERT_TRUE(catalog.InsertRow(UserInsertStatement(1, "alice")).ok);

  atlasdb::parser::UpdateStatement statement{
      "users",
      atlasdb::parser::Assignment{"name", atlasdb::parser::ValueLiteral{std::string{"alicia"}}},
      atlasdb::parser::EqualityPredicate{"id", atlasdb::parser::ValueLiteral{1LL}},
  };

  const atlasdb::catalog::CatalogStatus update = catalog.UpdateWhereEquals(statement);
  ASSERT_TRUE(update.ok);
  EXPECT_EQ(update.message, "updated 1 row in 'users'");

  const atlasdb::catalog::SelectResult selected = catalog.SelectAll(atlasdb::parser::SelectStatement{"users"});
  ASSERT_TRUE(selected.status.ok);
  ASSERT_EQ(selected.rows.size(), 1U);
  EXPECT_EQ(std::get<std::string>(selected.rows[0][1].value), "alicia");
}

TEST(MemoryCatalog, DeletesRowByPrimaryKey) {
  atlasdb::catalog::MemoryCatalog catalog;
  ASSERT_TRUE(catalog.CreateTable(UsersTableStatement()).ok);
  ASSERT_TRUE(catalog.InsertRow(UserInsertStatement(1, "alice")).ok);

  const atlasdb::catalog::CatalogStatus deletion = catalog.DeleteWhereEquals(atlasdb::parser::DeleteStatement{
      "users",
      atlasdb::parser::EqualityPredicate{"id", atlasdb::parser::ValueLiteral{1LL}},
  });

  ASSERT_TRUE(deletion.ok);
  EXPECT_EQ(deletion.message, "deleted 1 row from 'users'");
  EXPECT_EQ(catalog.RowCount("users"), 0U);
}

TEST(MemoryCatalog, RejectsUpdateWhereNonPrimaryKeyColumn) {
  atlasdb::catalog::MemoryCatalog catalog;
  ASSERT_TRUE(catalog.CreateTable(UsersTableStatement()).ok);
  ASSERT_TRUE(catalog.InsertRow(UserInsertStatement(1, "alice")).ok);

  const atlasdb::catalog::CatalogStatus update = catalog.UpdateWhereEquals(atlasdb::parser::UpdateStatement{
      "users",
      atlasdb::parser::Assignment{"name", atlasdb::parser::ValueLiteral{std::string{"alicia"}}},
      atlasdb::parser::EqualityPredicate{"name", atlasdb::parser::ValueLiteral{std::string{"alice"}}},
  });

  ASSERT_FALSE(update.ok);
  EXPECT_EQ(update.code, "E2008");
  EXPECT_EQ(update.message, "WHERE column must be PRIMARY KEY: name");
}

TEST(MemoryCatalog, RejectsDeleteWhenRowMissing) {
  atlasdb::catalog::MemoryCatalog catalog;
  ASSERT_TRUE(catalog.CreateTable(UsersTableStatement()).ok);

  const atlasdb::catalog::CatalogStatus deletion = catalog.DeleteWhereEquals(atlasdb::parser::DeleteStatement{
      "users",
      atlasdb::parser::EqualityPredicate{"id", atlasdb::parser::ValueLiteral{42LL}},
  });

  ASSERT_FALSE(deletion.ok);
  EXPECT_EQ(deletion.code, "E2009");
  EXPECT_EQ(deletion.message, "row not found for key match in table 'users'");
}

}  // namespace
