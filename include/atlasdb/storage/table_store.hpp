#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "atlasdb/storage/pager.hpp"
#include "atlasdb/storage/row_page.hpp"

namespace atlasdb::storage {

struct TableStoreStatus {
  bool ok;
  std::string code;
  std::string message;

  static TableStoreStatus Ok(std::string message = {});
  static TableStoreStatus Error(std::string code, std::string message);
};

struct TableRowLocation {
  std::uint32_t page_id{0};
  std::uint16_t slot_index{0};
};

class TableStore {
 public:
  explicit TableStore(Pager* pager);

  [[nodiscard]] TableStoreStatus Initialize(std::uint32_t* out_root_page);
  [[nodiscard]] TableStoreStatus AppendRow(std::uint32_t root_page_id,
                                           const std::vector<std::uint8_t>& row_bytes,
                                           TableRowLocation* out_location);
  [[nodiscard]] TableStoreStatus ReadRow(std::uint32_t root_page_id,
                                         const TableRowLocation& location,
                                         std::vector<std::uint8_t>* out_row_bytes);
  [[nodiscard]] TableStoreStatus RowCount(std::uint32_t root_page_id,
                                          std::uint32_t* out_row_count);

 private:
  Pager* pager_{nullptr};
};

}  // namespace atlasdb::storage
