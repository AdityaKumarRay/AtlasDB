#include "atlasdb/catalog/memory_catalog.hpp"

#include <cstdint>
#include <string>
#include <variant>
#include <vector>

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
  statement.values.emplace_back(id);
  statement.values.emplace_back(std::move(name));
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

TEST(MemoryCatalog, CreatesSecondaryIndexForKnownTableColumn) {
  atlasdb::catalog::MemoryCatalog catalog;
  ASSERT_TRUE(catalog.CreateTable(UsersTableStatement()).ok);

  const atlasdb::catalog::CatalogStatus create_index =
      catalog.CreateSecondaryIndex("users", "idx_users_name", "name");
  ASSERT_TRUE(create_index.ok);
  EXPECT_EQ(create_index.message, "created secondary index 'idx_users_name' on 'users.name'");

  const atlasdb::catalog::SecondaryIndexListResult list_result =
      catalog.ListSecondaryIndexes("users");
  ASSERT_TRUE(list_result.status.ok);
  ASSERT_EQ(list_result.indexes.size(), 1U);
  EXPECT_EQ(list_result.indexes[0].name, "idx_users_name");
  EXPECT_EQ(list_result.indexes[0].column_name, "name");
}

TEST(MemoryCatalog, RejectsSecondaryIndexForUnknownTable) {
  atlasdb::catalog::MemoryCatalog catalog;

  const atlasdb::catalog::CatalogStatus create_index =
      catalog.CreateSecondaryIndex("ghost", "idx_ghost_name", "name");
  ASSERT_FALSE(create_index.ok);
  EXPECT_EQ(create_index.code, "E2003");
  EXPECT_EQ(create_index.message, "table not found: ghost");
}

TEST(MemoryCatalog, RejectsSecondaryIndexForUnknownColumn) {
  atlasdb::catalog::MemoryCatalog catalog;
  ASSERT_TRUE(catalog.CreateTable(UsersTableStatement()).ok);

  const atlasdb::catalog::CatalogStatus create_index =
      catalog.CreateSecondaryIndex("users", "idx_users_email", "email");
  ASSERT_FALSE(create_index.ok);
  EXPECT_EQ(create_index.code, "E2012");
  EXPECT_EQ(create_index.message, "secondary index column not found: email");
}

TEST(MemoryCatalog, RejectsDuplicateSecondaryIndexNameIgnoringCase) {
  atlasdb::catalog::MemoryCatalog catalog;
  ASSERT_TRUE(catalog.CreateTable(UsersTableStatement()).ok);
  ASSERT_TRUE(catalog.CreateSecondaryIndex("users", "idx_users_name", "name").ok);

  const atlasdb::catalog::CatalogStatus duplicate_name =
      catalog.CreateSecondaryIndex("users", "IDX_USERS_NAME", "id");
  ASSERT_FALSE(duplicate_name.ok);
  EXPECT_EQ(duplicate_name.code, "E2013");
  EXPECT_EQ(duplicate_name.message, "secondary index already exists: IDX_USERS_NAME");
}

TEST(MemoryCatalog, RejectsDuplicateSecondaryIndexColumn) {
  atlasdb::catalog::MemoryCatalog catalog;
  ASSERT_TRUE(catalog.CreateTable(UsersTableStatement()).ok);
  ASSERT_TRUE(catalog.CreateSecondaryIndex("users", "idx_users_name", "name").ok);

  const atlasdb::catalog::CatalogStatus duplicate_column =
      catalog.CreateSecondaryIndex("users", "idx_users_name_dup", "name");
  ASSERT_FALSE(duplicate_column.ok);
  EXPECT_EQ(duplicate_column.code, "E2014");
  EXPECT_EQ(duplicate_column.message, "secondary index already exists on column: name");
}

TEST(MemoryCatalog, RejectsValueCountMismatch) {
  atlasdb::catalog::MemoryCatalog catalog;
  ASSERT_TRUE(catalog.CreateTable(UsersTableStatement()).ok);

  atlasdb::parser::InsertStatement statement;
  statement.table_name = "users";
  statement.values.emplace_back(1LL);

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
  statement.values.emplace_back(std::string{"not-int"});
  statement.values.emplace_back(std::string{"alice"});

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

TEST(MemoryCatalog, SnapshotRoundTripPreservesRows) {
  atlasdb::catalog::MemoryCatalog catalog;
  ASSERT_TRUE(catalog.CreateTable(UsersTableStatement()).ok);
  ASSERT_TRUE(catalog.InsertRow(UserInsertStatement(1, "alice")).ok);
  ASSERT_TRUE(catalog.InsertRow(UserInsertStatement(2, "bob")).ok);

  std::vector<std::uint8_t> bytes;
  const atlasdb::catalog::CatalogStatus serialize = catalog.Serialize(&bytes);
  ASSERT_TRUE(serialize.ok);
  ASSERT_FALSE(bytes.empty());

  atlasdb::catalog::MemoryCatalog restored;
  const atlasdb::catalog::CatalogStatus deserialize = restored.Deserialize(bytes);
  ASSERT_TRUE(deserialize.ok);

  const atlasdb::catalog::SelectResult selected = restored.SelectAll(atlasdb::parser::SelectStatement{"users"});
  ASSERT_TRUE(selected.status.ok);
  ASSERT_EQ(selected.rows.size(), 2U);
  EXPECT_EQ(std::get<std::int64_t>(selected.rows[0][0].value), 1);
  EXPECT_EQ(std::get<std::string>(selected.rows[0][1].value), "alice");
  EXPECT_EQ(std::get<std::int64_t>(selected.rows[1][0].value), 2);
  EXPECT_EQ(std::get<std::string>(selected.rows[1][1].value), "bob");
}

TEST(MemoryCatalog, SnapshotRoundTripPreservesSecondaryIndexes) {
  atlasdb::catalog::MemoryCatalog catalog;
  ASSERT_TRUE(catalog.CreateTable(UsersTableStatement()).ok);
  ASSERT_TRUE(catalog.CreateSecondaryIndex("users", "idx_users_name", "name").ok);

  std::vector<std::uint8_t> bytes;
  ASSERT_TRUE(catalog.Serialize(&bytes).ok);

  atlasdb::catalog::MemoryCatalog restored;
  ASSERT_TRUE(restored.Deserialize(bytes).ok);

  const atlasdb::catalog::SecondaryIndexListResult list_result =
      restored.ListSecondaryIndexes("users");
  ASSERT_TRUE(list_result.status.ok);
  ASSERT_EQ(list_result.indexes.size(), 1U);
  EXPECT_EQ(list_result.indexes[0].name, "idx_users_name");
  EXPECT_EQ(list_result.indexes[0].column_name, "name");
}

TEST(MemoryCatalog, SnapshotDeserializeSupportsLegacyVersionWithoutSecondaryIndexes) {
  const std::vector<std::uint8_t> bytes = {
      static_cast<std::uint8_t>('A'),
      static_cast<std::uint8_t>('T'),
      static_cast<std::uint8_t>('L'),
      static_cast<std::uint8_t>('C'),
      static_cast<std::uint8_t>('A'),
      static_cast<std::uint8_t>('T'),
      static_cast<std::uint8_t>('1'),
      0U,
      1U, 0U, 0U, 0U,
      0U, 0U, 0U, 0U,
  };

  atlasdb::catalog::MemoryCatalog catalog;
  const atlasdb::catalog::CatalogStatus deserialize = catalog.Deserialize(bytes);
  ASSERT_TRUE(deserialize.ok);
  EXPECT_FALSE(catalog.HasTable("users"));
}

TEST(MemoryCatalog, SnapshotSerializeRejectsNullOutputBuffer) {
  atlasdb::catalog::MemoryCatalog catalog;
  const atlasdb::catalog::CatalogStatus serialize = catalog.Serialize(nullptr);

  ASSERT_FALSE(serialize.ok);
  EXPECT_EQ(serialize.code, "E2020");
  EXPECT_EQ(serialize.message, "output snapshot pointer is null");
}

TEST(MemoryCatalog, SnapshotDeserializeRejectsUnsupportedVersion) {
  atlasdb::catalog::MemoryCatalog catalog;
  ASSERT_TRUE(catalog.CreateTable(UsersTableStatement()).ok);

  std::vector<std::uint8_t> bytes;
  ASSERT_TRUE(catalog.Serialize(&bytes).ok);
  ASSERT_GE(bytes.size(), 12U);

  bytes[8] = 3U;
  bytes[9] = 0U;
  bytes[10] = 0U;
  bytes[11] = 0U;

  atlasdb::catalog::MemoryCatalog restored;
  const atlasdb::catalog::CatalogStatus deserialize = restored.Deserialize(bytes);

  ASSERT_FALSE(deserialize.ok);
  EXPECT_EQ(deserialize.code, "E2022");
  EXPECT_EQ(deserialize.message, "unsupported catalog snapshot version");
}

TEST(MemoryCatalog, SnapshotDeserializeRejectsTrailingBytes) {
  atlasdb::catalog::MemoryCatalog catalog;
  ASSERT_TRUE(catalog.CreateTable(UsersTableStatement()).ok);

  std::vector<std::uint8_t> bytes;
  ASSERT_TRUE(catalog.Serialize(&bytes).ok);
  bytes.push_back(0xFFU);

  atlasdb::catalog::MemoryCatalog restored;
  const atlasdb::catalog::CatalogStatus deserialize = restored.Deserialize(bytes);

  ASSERT_FALSE(deserialize.ok);
  EXPECT_EQ(deserialize.code, "E2027");
  EXPECT_EQ(deserialize.message, "catalog snapshot contains trailing bytes");
}

}  // namespace
