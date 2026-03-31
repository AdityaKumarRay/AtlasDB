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
  const atlasdb::Status status = engine.Execute("SELECT 1");

  EXPECT_FALSE(status.ok);
  EXPECT_EQ(status.message, "E1200: unsupported statement; expected CREATE TABLE or INSERT INTO");
}

TEST(VersionSmoke, VersionStringIsPresent) {
  EXPECT_FALSE(atlasdb::kVersion.empty());
}

}  // namespace
