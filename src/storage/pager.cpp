#include "atlasdb/storage/pager.hpp"

#include <cstdint>
#include <filesystem>
#include <limits>
#include <string>
#include <system_error>
#include <utility>

namespace atlasdb::storage {
namespace {

constexpr std::streamsize kPageSizeStream = static_cast<std::streamsize>(kPageSize);

std::streamoff PageOffset(std::uint32_t page_id) {
  return static_cast<std::streamoff>(static_cast<std::uint64_t>(page_id) *
                                     static_cast<std::uint64_t>(kPageSize));
}

}  // namespace

PagerStatus PagerStatus::Ok(std::string message) {
  return PagerStatus{true, "", std::move(message)};
}

PagerStatus PagerStatus::Error(std::string code, std::string message) {
  return PagerStatus{false, std::move(code), std::move(message)};
}

Pager::~Pager() {
  Close();
}

PagerStatus Pager::Open(const std::string& file_path) {
  if (is_open_) {
    Close();
  }

  if (file_path.empty()) {
    return PagerStatus::Error("E3101", "database file path is empty");
  }

  const std::filesystem::path path(file_path);
  std::error_code path_error;

  const bool file_exists = std::filesystem::exists(path, path_error);
  if (path_error) {
    return PagerStatus::Error("E3101", "failed to inspect database file path");
  }

  if (!path.parent_path().empty()) {
    std::filesystem::create_directories(path.parent_path(), path_error);
    if (path_error) {
      return PagerStatus::Error("E3101", "failed to create database directory");
    }
  }

  std::ios::openmode mode = std::ios::binary | std::ios::in | std::ios::out;
  if (!file_exists) {
    mode |= std::ios::trunc;
  }

  file_.open(path, mode);
  if (!file_.is_open()) {
    return PagerStatus::Error("E3101", "failed to open database file");
  }

  file_path_ = path;
  is_open_ = true;

  if (!file_exists) {
    header_ = DatabaseFileHeader{};
    const PagerStatus flush_status = FlushHeader();
    if (!flush_status.ok) {
      Close();
      return flush_status;
    }

    return PagerStatus::Ok("created new database file");
  }

  Page header_page;
  const PagerStatus read_status = ReadPageRaw(0U, &header_page);
  if (!read_status.ok) {
    Close();
    return read_status;
  }

  const HeaderStatus header_status = DeserializeFileHeaderPage(header_page, &header_);
  if (!header_status.ok) {
    Close();
    return PagerStatus::Error(header_status.code, header_status.message);
  }

  file_.clear();
  file_.seekg(0, std::ios::end);
  if (!file_) {
    Close();
    return PagerStatus::Error("E3102", "failed to read database file size");
  }

  const std::streampos end_position = file_.tellg();
  if (end_position == static_cast<std::streampos>(-1)) {
    Close();
    return PagerStatus::Error("E3102", "failed to read database file size");
  }

  const std::uint64_t file_size = static_cast<std::uint64_t>(end_position);
  const std::uint64_t declared_size = static_cast<std::uint64_t>(header_.page_count) *
                                      static_cast<std::uint64_t>(kPageSize);
  if (file_size < declared_size) {
    Close();
    return PagerStatus::Error("E3103", "database file is smaller than declared page count");
  }

  return PagerStatus::Ok("opened existing database file");
}

void Pager::Close() {
  if (file_.is_open()) {
    file_.clear();
    file_.flush();
    file_.close();
  }

  file_.clear();
  file_path_.clear();
  header_ = DatabaseFileHeader{};
  is_open_ = false;
}

bool Pager::IsOpen() const noexcept {
  return is_open_;
}

const DatabaseFileHeader& Pager::Header() const noexcept {
  return header_;
}

PagerStatus Pager::ReadPage(std::uint32_t page_id, Page* out_page) {
  if (out_page == nullptr) {
    return PagerStatus::Error("E3100", "output page pointer is null");
  }

  if (!is_open_) {
    return PagerStatus::Error("E3101", "pager is not open");
  }

  if (page_id >= header_.page_count) {
    return PagerStatus::Error("E3104", "page id is outside declared page count");
  }

  return ReadPageRaw(page_id, out_page);
}

PagerStatus Pager::WritePage(const Page& page) {
  if (!is_open_) {
    return PagerStatus::Error("E3101", "pager is not open");
  }

  if (page.id >= header_.page_count) {
    return PagerStatus::Error("E3106", "page id is outside declared page count");
  }

  return WritePageRaw(page);
}

PagerStatus Pager::AllocatePage(std::uint32_t* out_page_id) {
  if (out_page_id == nullptr) {
    return PagerStatus::Error("E3100", "output page id pointer is null");
  }

  if (!is_open_) {
    return PagerStatus::Error("E3101", "pager is not open");
  }

  if (header_.page_count == std::numeric_limits<std::uint32_t>::max()) {
    return PagerStatus::Error("E3110", "page id space exhausted");
  }

  const std::uint32_t page_id = header_.page_count;
  const Page new_page = CreateZeroedPage(page_id);

  const PagerStatus write_status = WritePageRaw(new_page);
  if (!write_status.ok) {
    return write_status;
  }

  header_.page_count += 1U;
  const PagerStatus flush_status = FlushHeader();
  if (!flush_status.ok) {
    return flush_status;
  }

  *out_page_id = page_id;
  return PagerStatus::Ok("allocated page");
}

PagerStatus Pager::UpdateCatalogMetadata(std::uint32_t catalog_root_page, std::uint64_t schema_epoch) {
  if (!is_open_) {
    return PagerStatus::Error("E3101", "pager is not open");
  }

  if (catalog_root_page != 0U && catalog_root_page >= header_.page_count) {
    return PagerStatus::Error("E3108", "catalog root page is outside declared page count");
  }

  header_.catalog_root_page = catalog_root_page;
  header_.schema_epoch = schema_epoch;
  return FlushHeader();
}

PagerStatus Pager::ReadPageRaw(std::uint32_t page_id, Page* out_page) {
  if (out_page == nullptr) {
    return PagerStatus::Error("E3100", "output page pointer is null");
  }

  file_.clear();
  file_.seekg(PageOffset(page_id), std::ios::beg);
  if (!file_) {
    return PagerStatus::Error("E3102", "failed to seek page for read");
  }

  Page page = CreateZeroedPage(page_id);
  file_.read(reinterpret_cast<char*>(page.bytes.data()), kPageSizeStream);
  const std::streamsize bytes_read = file_.gcount();
  if (bytes_read != kPageSizeStream) {
    file_.clear();
    return PagerStatus::Error("E3102", "failed to read full page bytes");
  }

  *out_page = page;
  return PagerStatus::Ok();
}

PagerStatus Pager::WritePageRaw(const Page& page) {
  file_.clear();
  file_.seekp(PageOffset(page.id), std::ios::beg);
  if (!file_) {
    return PagerStatus::Error("E3107", "failed to seek page for write");
  }

  file_.write(reinterpret_cast<const char*>(page.bytes.data()), kPageSizeStream);
  if (!file_) {
    return PagerStatus::Error("E3107", "failed to write page bytes");
  }

  file_.flush();
  if (!file_) {
    return PagerStatus::Error("E3107", "failed to flush page write");
  }

  return PagerStatus::Ok();
}

PagerStatus Pager::FlushHeader() {
  const Page header_page = SerializeFileHeaderPage(header_);
  return WritePageRaw(header_page);
}

}  // namespace atlasdb::storage
