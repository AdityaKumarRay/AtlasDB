#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "atlasdb/parser/ast.hpp"

namespace atlasdb::planner {

enum class PlanOperation {
  CreateTable,
  Insert,
  Select,
  Update,
  Delete,
};

enum class PlanAccessPath {
  None,
  TableScan,
  PrimaryKeyIndexLookup,
};

struct TablePlanningMetadata {
  struct SecondaryIndexMetadata {
    std::string index_name;
    std::string column_name;
  };

  std::string table_name;
  std::string primary_key_column;
  bool has_primary_key_index{false};
  std::vector<SecondaryIndexMetadata> secondary_indexes;
};

struct QueryPlan {
  PlanOperation operation{PlanOperation::Select};
  PlanAccessPath access_path{PlanAccessPath::None};
  std::string table_name;
  bool maintain_primary_key_index{false};
  bool maintain_secondary_indexes{false};
};

struct PlannerStatus {
  bool ok;
  std::string code;
  std::string message;

  static PlannerStatus Ok(std::string message = {});
  static PlannerStatus Error(std::string code, std::string message);
};

class RulePlanner {
 public:
  [[nodiscard]] PlannerStatus Plan(const parser::Statement& statement,
                                   const std::vector<TablePlanningMetadata>& table_metadata,
                                   QueryPlan* out_plan) const;

 private:
  [[nodiscard]] const TablePlanningMetadata* FindTableMetadata(
      std::string_view table_name,
      const std::vector<TablePlanningMetadata>& table_metadata) const;
};

}  // namespace atlasdb::planner
