#include "atlasdb/storage/page.hpp"

namespace atlasdb::storage {

Page CreateZeroedPage(std::uint32_t page_id) {
  Page page;
  page.id = page_id;
  page.bytes.fill(0U);
  return page;
}

}  // namespace atlasdb::storage
