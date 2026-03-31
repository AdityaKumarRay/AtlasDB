#include "atlasdb/database.hpp"
#include "atlasdb/version.hpp"

#include <gtest/gtest.h>

namespace {

TEST(DatabaseEngineSmoke, AcceptsCreateTableStatement) {
  atlasdb::DatabaseEngine engine;
  const atlasdb::Status status = engine.Execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);");

  EXPECT_TRUE(status.ok);
  EXPECT_EQ(status.message, "accepted CREATE TABLE for table 'users'");
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
