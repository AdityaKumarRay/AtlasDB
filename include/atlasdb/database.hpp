#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

#include "atlasdb/catalog/memory_catalog.hpp"
#include "atlasdb/storage/pager.hpp"

namespace atlasdb {

struct Status {
  bool ok;
  std::string message;

  static Status Ok(std::string message = {});
  static Status Error(std::string message);
};

class DatabaseEngine {
 public:
  DatabaseEngine() = default;
  explicit DatabaseEngine(std::string database_path);

  [[nodiscard]] Status Execute(std::string_view statement);
  [[nodiscard]] std::string_view LastMessage() const noexcept;

 private:
  [[nodiscard]] Status LoadCatalogSnapshotFromPager();
  [[nodiscard]] Status PersistCatalogSnapshotToPager();
  [[nodiscard]] Status RebuildTableStoresFromCatalog();
  [[nodiscard]] Status RebuildSingleTableStore(std::string_view table_name);
  [[nodiscard]] Status InitializeCreateTableStore(const parser::CreateTableStatement& statement);
  [[nodiscard]] Status AppendInsertToTableStore(const parser::InsertStatement& statement);

  catalog::MemoryCatalog catalog_;
  std::unique_ptr<storage::Pager> pager_{};
  std::unordered_map<std::string, std::uint32_t> table_store_roots_{};
  std::uint64_t schema_epoch_{0};
  bool persistence_enabled_{false};
  std::string startup_error_{};
  std::string last_message_{"AtlasDB initialized."};
};

}  // namespace atlasdb
