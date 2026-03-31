#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace atlasdb::storage {

inline constexpr std::size_t kPageSize = 4096;

struct Page {
  std::uint32_t id{0};
  std::array<std::uint8_t, kPageSize> bytes{};
};

[[nodiscard]] Page CreateZeroedPage(std::uint32_t page_id);

}  // namespace atlasdb::storage
