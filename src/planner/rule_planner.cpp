#include "atlasdb/planner/rule_planner.hpp"

#include <algorithm>
#include <cctype>
#include <string>
#include <string_view>
#include <utility>

namespace atlasdb::planner {
namespace {

std::string NormalizeIdentifier(std::string_view identifier) {
  std::string normalized(identifier);
  std::transform(normalized.begin(), normalized.end(), normalized.begin(), [](unsigned char value) {
    return static_cast<char>(std::tolower(value));
  });
  return normalized;
}

}  // namespace

PlannerStatus PlannerStatus::Ok(std::string message) {
  return PlannerStatus{true, "", std::move(message)};
}

PlannerStatus PlannerStatus::Error(std::string code, std::string message) {
  return PlannerStatus{false, std::move(code), std::move(message)};
}

const TablePlanningMetadata* RulePlanner::FindTableMetadata(
    std::string_view table_name,
    const std::vector<TablePlanningMetadata>& table_metadata) const {
  const std::string normalized_target = NormalizeIdentifier(table_name);
  for (const TablePlanningMetadata& metadata : table_metadata) {
    if (NormalizeIdentifier(metadata.table_name) == normalized_target) {
      return &metadata;
    }
  }

  return nullptr;
}

PlannerStatus RulePlanner::Plan(const parser::Statement& statement,
                                const std::vector<TablePlanningMetadata>& table_metadata,
                                QueryPlan* out_plan) const {
  if (out_plan == nullptr) {
    return PlannerStatus::Error("E6100", "output query plan pointer is null");
  }

  QueryPlan plan;

  if (std::holds_alternative<parser::CreateTableStatement>(statement)) {
    const auto& create_statement = std::get<parser::CreateTableStatement>(statement);
    plan.operation = PlanOperation::CreateTable;
    plan.access_path = PlanAccessPath::None;
    plan.table_name = create_statement.table_name;
    *out_plan = std::move(plan);
    return PlannerStatus::Ok("planned CREATE TABLE with direct DDL path");
  }

  if (std::holds_alternative<parser::InsertStatement>(statement)) {
    const auto& insert_statement = std::get<parser::InsertStatement>(statement);
    plan.operation = PlanOperation::Insert;
    plan.access_path = PlanAccessPath::None;
    plan.table_name = insert_statement.table_name;

    const TablePlanningMetadata* metadata =
        FindTableMetadata(insert_statement.table_name, table_metadata);
    if (metadata != nullptr) {
      plan.maintain_primary_key_index = metadata->has_primary_key_index;
      plan.maintain_secondary_indexes = !metadata->secondary_indexes.empty();
    }

    *out_plan = std::move(plan);
    return PlannerStatus::Ok("planned INSERT mutation path");
  }

  if (std::holds_alternative<parser::SelectStatement>(statement)) {
    const auto& select_statement = std::get<parser::SelectStatement>(statement);
    const TablePlanningMetadata* metadata =
        FindTableMetadata(select_statement.table_name, table_metadata);
    if (metadata == nullptr) {
      return PlannerStatus::Error("E6102", "table metadata not found for planner: " +
                                               select_statement.table_name);
    }

    plan.operation = PlanOperation::Select;
    plan.access_path = PlanAccessPath::TableScan;
    plan.table_name = select_statement.table_name;
    *out_plan = std::move(plan);
    return PlannerStatus::Ok("planned SELECT with table scan path");
  }

  if (std::holds_alternative<parser::UpdateStatement>(statement)) {
    const auto& update_statement = std::get<parser::UpdateStatement>(statement);
    const TablePlanningMetadata* metadata =
        FindTableMetadata(update_statement.table_name, table_metadata);
    if (metadata == nullptr) {
      return PlannerStatus::Error("E6102", "table metadata not found for planner: " +
                                               update_statement.table_name);
    }

    if (metadata->primary_key_column.empty()) {
      return PlannerStatus::Error("E6103", "table metadata missing primary key column: " +
                                               update_statement.table_name);
    }

    plan.operation = PlanOperation::Update;
    plan.table_name = update_statement.table_name;

    const bool predicate_is_primary =
        NormalizeIdentifier(update_statement.predicate.column_name) ==
        NormalizeIdentifier(metadata->primary_key_column);
    plan.access_path =
        (metadata->has_primary_key_index && predicate_is_primary)
            ? PlanAccessPath::PrimaryKeyIndexLookup
            : PlanAccessPath::TableScan;

    *out_plan = std::move(plan);
    return PlannerStatus::Ok("planned UPDATE access path");
  }

  const auto& delete_statement = std::get<parser::DeleteStatement>(statement);
  const TablePlanningMetadata* metadata =
      FindTableMetadata(delete_statement.table_name, table_metadata);
  if (metadata == nullptr) {
    return PlannerStatus::Error("E6102", "table metadata not found for planner: " +
                                             delete_statement.table_name);
  }

  if (metadata->primary_key_column.empty()) {
    return PlannerStatus::Error("E6103", "table metadata missing primary key column: " +
                                             delete_statement.table_name);
  }

  plan.operation = PlanOperation::Delete;
  plan.table_name = delete_statement.table_name;

  const bool predicate_is_primary =
      NormalizeIdentifier(delete_statement.predicate.column_name) ==
      NormalizeIdentifier(metadata->primary_key_column);
  plan.access_path =
      (metadata->has_primary_key_index && predicate_is_primary)
          ? PlanAccessPath::PrimaryKeyIndexLookup
          : PlanAccessPath::TableScan;

  *out_plan = std::move(plan);
  return PlannerStatus::Ok("planned DELETE access path");
}

}  // namespace atlasdb::planner
