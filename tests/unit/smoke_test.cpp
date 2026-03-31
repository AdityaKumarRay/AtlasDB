#include "atlasdb/database.hpp"
#include "atlasdb/version.hpp"

#include <gtest/gtest.h>

namespace {

TEST(DatabaseEngineSmoke, AcceptsCreateTableStatement) {
  atlasdb::DatabaseEngine engine;
  const atlasdb::Status status = engine.Execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);");

  EXPECT_TRUE(status.ok);
  EXPECT_EQ(status.message, "created table 'users'");
}

TEST(DatabaseEngineSmoke, InsertsRowAfterCreateTable) {
  atlasdb::DatabaseEngine engine;
  ASSERT_TRUE(engine.Execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);").ok);

  const atlasdb::Status status = engine.Execute("INSERT INTO users VALUES (1, 'alice');");
  EXPECT_TRUE(status.ok);
  EXPECT_EQ(status.message, "inserted 1 row into 'users'");
}

TEST(DatabaseEngineSmoke, RejectsDuplicatePrimaryKey) {
  atlasdb::DatabaseEngine engine;
  ASSERT_TRUE(engine.Execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);").ok);
  ASSERT_TRUE(engine.Execute("INSERT INTO users VALUES (1, 'alice');").ok);

  const atlasdb::Status duplicate = engine.Execute("INSERT INTO users VALUES (1, 'bob');");
  EXPECT_FALSE(duplicate.ok);
  EXPECT_EQ(duplicate.message, "E2006: duplicate primary key for table 'users'");
}

TEST(DatabaseEngineSmoke, RejectsInsertForUnknownTable) {
  atlasdb::DatabaseEngine engine;

  const atlasdb::Status status = engine.Execute("INSERT INTO users VALUES (1, 'alice');");
  EXPECT_FALSE(status.ok);
  EXPECT_EQ(status.message, "E2003: table not found: users");
}

TEST(DatabaseEngineSmoke, SelectsRowsAfterInsert) {
  atlasdb::DatabaseEngine engine;
  ASSERT_TRUE(engine.Execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);").ok);
  ASSERT_TRUE(engine.Execute("INSERT INTO users VALUES (1, 'alice');").ok);
  ASSERT_TRUE(engine.Execute("INSERT INTO users VALUES (2, 'bob');").ok);

  const atlasdb::Status select = engine.Execute("SELECT * FROM users;");
  EXPECT_TRUE(select.ok);
  EXPECT_EQ(select.message, "selected 2 row(s) from 'users': [1, 'alice']; [2, 'bob']");
}

TEST(DatabaseEngineSmoke, RejectsSelectForUnknownTable) {
  atlasdb::DatabaseEngine engine;

  const atlasdb::Status select = engine.Execute("SELECT * FROM users;");
  EXPECT_FALSE(select.ok);
  EXPECT_EQ(select.message, "E2003: table not found: users");
}

TEST(DatabaseEngineSmoke, UpdatesRowByPrimaryKey) {
  atlasdb::DatabaseEngine engine;
  ASSERT_TRUE(engine.Execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);").ok);
  ASSERT_TRUE(engine.Execute("INSERT INTO users VALUES (1, 'alice');").ok);

  const atlasdb::Status update = engine.Execute("UPDATE users SET name = 'alicia' WHERE id = 1;");
  EXPECT_TRUE(update.ok);
  EXPECT_EQ(update.message, "updated 1 row in 'users'");

  const atlasdb::Status select = engine.Execute("SELECT * FROM users;");
  EXPECT_TRUE(select.ok);
  EXPECT_EQ(select.message, "selected 1 row(s) from 'users': [1, 'alicia']");
}

TEST(DatabaseEngineSmoke, DeletesRowByPrimaryKey) {
  atlasdb::DatabaseEngine engine;
  ASSERT_TRUE(engine.Execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);").ok);
  ASSERT_TRUE(engine.Execute("INSERT INTO users VALUES (1, 'alice');").ok);

  const atlasdb::Status deletion = engine.Execute("DELETE FROM users WHERE id = 1;");
  EXPECT_TRUE(deletion.ok);
  EXPECT_EQ(deletion.message, "deleted 1 row from 'users'");

  const atlasdb::Status select = engine.Execute("SELECT * FROM users;");
  EXPECT_TRUE(select.ok);
  EXPECT_EQ(select.message, "selected 0 row(s) from 'users'");
}

TEST(DatabaseEngineSmoke, RejectsUpdateWhereNonPrimaryKey) {
  atlasdb::DatabaseEngine engine;
  ASSERT_TRUE(engine.Execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);").ok);
  ASSERT_TRUE(engine.Execute("INSERT INTO users VALUES (1, 'alice');").ok);

  const atlasdb::Status update = engine.Execute("UPDATE users SET name = 'alicia' WHERE name = 'alice';");
  EXPECT_FALSE(update.ok);
  EXPECT_EQ(update.message, "E2008: WHERE column must be PRIMARY KEY: name");
}

TEST(DatabaseEngineSmoke, RejectsEmptyStatement) {
  atlasdb::DatabaseEngine engine;
  const atlasdb::Status status = engine.Execute("   ");

  EXPECT_FALSE(status.ok);
  EXPECT_EQ(status.message, "E0001: empty statement");
}

TEST(DatabaseEngineSmoke, ReportsUnknownMetaCommand) {
  atlasdb::DatabaseEngine engine;
  const atlasdb::Status status = engine.Execute(".unknown");

  EXPECT_FALSE(status.ok);
  EXPECT_EQ(status.message, "E0002: unknown meta command");
}

TEST(DatabaseEngineSmoke, RejectsUnsupportedStatement) {
  atlasdb::DatabaseEngine engine;
  const atlasdb::Status status = engine.Execute("MERGE users;");

  EXPECT_FALSE(status.ok);
  EXPECT_EQ(status.message,
            "E1200: unsupported statement; expected CREATE TABLE, INSERT INTO, SELECT * FROM, UPDATE, or DELETE FROM");
}

TEST(VersionSmoke, VersionStringIsPresent) {
  EXPECT_FALSE(atlasdb::kVersion.empty());
}

}  // namespace
