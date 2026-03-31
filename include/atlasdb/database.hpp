#pragma once

#include <string>
#include <string_view>

#include "atlasdb/catalog/memory_catalog.hpp"

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

  [[nodiscard]] Status Execute(std::string_view statement);
  [[nodiscard]] std::string_view LastMessage() const noexcept;

 private:
    catalog::MemoryCatalog catalog_;
  std::string last_message_{"AtlasDB initialized."};
};

}  // namespace atlasdb
