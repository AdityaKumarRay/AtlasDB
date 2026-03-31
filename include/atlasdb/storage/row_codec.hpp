#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "atlasdb/parser/ast.hpp"

namespace atlasdb::storage {

struct RowCodecStatus {
  bool ok;
  std::string code;
  std::string message;

  static RowCodecStatus Ok(std::string message = {});
  static RowCodecStatus Error(std::string code, std::string message);
};

[[nodiscard]] RowCodecStatus SerializeRow(const std::vector<parser::ColumnDefinition>& columns,
                                          const std::vector<parser::ValueLiteral>& values,
                                          std::vector<std::uint8_t>* out_bytes);

[[nodiscard]] RowCodecStatus DeserializeRow(const std::vector<parser::ColumnDefinition>& columns,
                                            const std::vector<std::uint8_t>& bytes,
                                            std::vector<parser::ValueLiteral>* out_values);

}  // namespace atlasdb::storage
