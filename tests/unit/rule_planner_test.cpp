#include "atlasdb/planner/rule_planner.hpp"

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "atlasdb/parser/ast.hpp"

namespace {

std::vector<atlasdb::planner::TablePlanningMetadata> MetadataForUsers(bool has_primary_index) {
  return {
      {"users", "id", has_primary_index},
  };
}

TEST(RulePlanner, PlansCreateTableWithDirectDdlPath) {
  atlasdb::planner::RulePlanner planner;
  atlasdb::planner::QueryPlan plan;

  const atlasdb::parser::CreateTableStatement statement{
      "users",
      {
          {"id", atlasdb::parser::ColumnType::Integer, true},
          {"name", atlasdb::parser::ColumnType::Text, false},
      },
  };

  const atlasdb::planner::PlannerStatus status = planner.Plan(statement, {}, &plan);
  ASSERT_TRUE(status.ok);
  EXPECT_EQ(plan.operation, atlasdb::planner::PlanOperation::CreateTable);
  EXPECT_EQ(plan.access_path, atlasdb::planner::PlanAccessPath::None);
  EXPECT_EQ(plan.table_name, "users");
}

TEST(RulePlanner, PlansInsertAndTracksPrimaryIndexMaintenanceIntent) {
  atlasdb::planner::RulePlanner planner;
  atlasdb::planner::QueryPlan plan;

  const atlasdb::parser::InsertStatement statement{
      "users",
      {
          atlasdb::parser::ValueLiteral{1},
          atlasdb::parser::ValueLiteral{std::string("alice")},
      },
  };

  const atlasdb::planner::PlannerStatus status =
      planner.Plan(statement, MetadataForUsers(true), &plan);
  ASSERT_TRUE(status.ok);
  EXPECT_EQ(plan.operation, atlasdb::planner::PlanOperation::Insert);
  EXPECT_TRUE(plan.maintain_primary_key_index);
}

TEST(RulePlanner, PlansSelectAsTableScan) {
  atlasdb::planner::RulePlanner planner;
  atlasdb::planner::QueryPlan plan;

  const atlasdb::parser::SelectStatement statement{"users"};
  const atlasdb::planner::PlannerStatus status =
      planner.Plan(statement, MetadataForUsers(true), &plan);

  ASSERT_TRUE(status.ok);
  EXPECT_EQ(plan.operation, atlasdb::planner::PlanOperation::Select);
  EXPECT_EQ(plan.access_path, atlasdb::planner::PlanAccessPath::TableScan);
}

TEST(RulePlanner, PlansUpdateWithPrimaryKeyIndexLookupWhenAvailable) {
  atlasdb::planner::RulePlanner planner;
  atlasdb::planner::QueryPlan plan;

  const atlasdb::parser::UpdateStatement statement{
      "users",
      {"name", atlasdb::parser::ValueLiteral{std::string("alicia")}},
      {"id", atlasdb::parser::ValueLiteral{1}},
  };

  const atlasdb::planner::PlannerStatus status =
      planner.Plan(statement, MetadataForUsers(true), &plan);

  ASSERT_TRUE(status.ok);
  EXPECT_EQ(plan.operation, atlasdb::planner::PlanOperation::Update);
  EXPECT_EQ(plan.access_path, atlasdb::planner::PlanAccessPath::PrimaryKeyIndexLookup);
}

TEST(RulePlanner, PlansDeleteAsTableScanWhenIndexMissing) {
  atlasdb::planner::RulePlanner planner;
  atlasdb::planner::QueryPlan plan;

  const atlasdb::parser::DeleteStatement statement{
      "users",
      {"id", atlasdb::parser::ValueLiteral{1}},
  };

  const atlasdb::planner::PlannerStatus status =
      planner.Plan(statement, MetadataForUsers(false), &plan);

  ASSERT_TRUE(status.ok);
  EXPECT_EQ(plan.operation, atlasdb::planner::PlanOperation::Delete);
  EXPECT_EQ(plan.access_path, atlasdb::planner::PlanAccessPath::TableScan);
}

TEST(RulePlanner, FallsBackToTableScanWhenPredicateIsNotPrimaryKey) {
  atlasdb::planner::RulePlanner planner;
  atlasdb::planner::QueryPlan plan;

  const atlasdb::parser::UpdateStatement statement{
      "users",
      {"name", atlasdb::parser::ValueLiteral{std::string("alicia")}},
      {"name", atlasdb::parser::ValueLiteral{std::string("alice")}},
  };

  const atlasdb::planner::PlannerStatus status =
      planner.Plan(statement, MetadataForUsers(true), &plan);

  ASSERT_TRUE(status.ok);
  EXPECT_EQ(plan.access_path, atlasdb::planner::PlanAccessPath::TableScan);
}

TEST(RulePlanner, RejectsMissingTableMetadataForSelectPlanning) {
  atlasdb::planner::RulePlanner planner;
  atlasdb::planner::QueryPlan plan;

  const atlasdb::parser::SelectStatement statement{"users"};
  const atlasdb::planner::PlannerStatus status = planner.Plan(statement, {}, &plan);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E6102");
}

TEST(RulePlanner, RejectsNullOutputPlanPointer) {
  atlasdb::planner::RulePlanner planner;

  const atlasdb::parser::SelectStatement statement{"users"};
  const atlasdb::planner::PlannerStatus status =
      planner.Plan(statement, MetadataForUsers(true), nullptr);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E6100");
}

}  // namespace
