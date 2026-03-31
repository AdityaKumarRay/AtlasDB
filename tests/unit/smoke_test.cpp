#include "atlasdb/database.hpp"
#include "atlasdb/version.hpp"

#include <gtest/gtest.h>

namespace {

TEST(DatabaseEngineSmoke, AcceptsBasicStatement) {
  atlasdb::DatabaseEngine engine;
  const atlasdb::Status status = engine.Execute("select 1");

  EXPECT_TRUE(status.ok);
  EXPECT_EQ(status.message, "accepted statement: select 1");
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

TEST(VersionSmoke, VersionStringIsPresent) {
  EXPECT_FALSE(atlasdb::kVersion.empty());
}

}  // namespace
