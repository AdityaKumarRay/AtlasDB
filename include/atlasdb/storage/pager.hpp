#pragma once

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>

#include "atlasdb/storage/file_header.hpp"
#include "atlasdb/storage/page.hpp"

namespace atlasdb::storage {

struct PagerStatus {
  bool ok;
  std::string code;
  std::string message;

  static PagerStatus Ok(std::string message = {});
  static PagerStatus Error(std::string code, std::string message);
};

class Pager {
 public:
  Pager() = default;
  ~Pager();

  Pager(const Pager&) = delete;
  Pager& operator=(const Pager&) = delete;

  [[nodiscard]] PagerStatus Open(const std::string& file_path);
  void Close();

  [[nodiscard]] bool IsOpen() const noexcept;
  [[nodiscard]] const DatabaseFileHeader& Header() const noexcept;

  [[nodiscard]] PagerStatus ReadPage(std::uint32_t page_id, Page* out_page);
  [[nodiscard]] PagerStatus WritePage(const Page& page);
  [[nodiscard]] PagerStatus AllocatePage(std::uint32_t* out_page_id);
  [[nodiscard]] PagerStatus UpdateCatalogMetadata(std::uint32_t catalog_root_page,
                                                  std::uint64_t schema_epoch);

 private:
  [[nodiscard]] PagerStatus ReadPageRaw(std::uint32_t page_id, Page* out_page);
  [[nodiscard]] PagerStatus WritePageRaw(const Page& page);
  [[nodiscard]] PagerStatus FlushHeader();

  std::filesystem::path file_path_{};
  std::fstream file_{};
  DatabaseFileHeader header_{};
  bool is_open_{false};
};

}  // namespace atlasdb::storage
