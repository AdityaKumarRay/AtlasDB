#include "atlasdb/storage/row_codec.hpp"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace atlasdb::storage {
namespace {

constexpr std::uint8_t kIntegerTag = 1U;
constexpr std::uint8_t kTextTag = 2U;

void WriteUint8(std::vector<std::uint8_t>* out_bytes, std::uint8_t value) {
  out_bytes->push_back(value);
}

void WriteUint16(std::vector<std::uint8_t>* out_bytes, std::uint16_t value) {
  out_bytes->push_back(static_cast<std::uint8_t>(value & 0xFFU));
  out_bytes->push_back(static_cast<std::uint8_t>((value >> 8U) & 0xFFU));
}

void WriteUint32(std::vector<std::uint8_t>* out_bytes, std::uint32_t value) {
  out_bytes->push_back(static_cast<std::uint8_t>(value & 0xFFU));
  out_bytes->push_back(static_cast<std::uint8_t>((value >> 8U) & 0xFFU));
  out_bytes->push_back(static_cast<std::uint8_t>((value >> 16U) & 0xFFU));
  out_bytes->push_back(static_cast<std::uint8_t>((value >> 24U) & 0xFFU));
}

void WriteUint64(std::vector<std::uint8_t>* out_bytes, std::uint64_t value) {
  out_bytes->push_back(static_cast<std::uint8_t>(value & 0xFFU));
  out_bytes->push_back(static_cast<std::uint8_t>((value >> 8U) & 0xFFU));
  out_bytes->push_back(static_cast<std::uint8_t>((value >> 16U) & 0xFFU));
  out_bytes->push_back(static_cast<std::uint8_t>((value >> 24U) & 0xFFU));
  out_bytes->push_back(static_cast<std::uint8_t>((value >> 32U) & 0xFFU));
  out_bytes->push_back(static_cast<std::uint8_t>((value >> 40U) & 0xFFU));
  out_bytes->push_back(static_cast<std::uint8_t>((value >> 48U) & 0xFFU));
  out_bytes->push_back(static_cast<std::uint8_t>((value >> 56U) & 0xFFU));
}

bool ReadUint8(const std::vector<std::uint8_t>& bytes, std::size_t* offset, std::uint8_t* value) {
  if (offset == nullptr || value == nullptr) {
    return false;
  }

  if (*offset + 1U > bytes.size()) {
    return false;
  }

  *value = bytes[*offset];
  *offset += 1U;
  return true;
}

bool ReadUint16(const std::vector<std::uint8_t>& bytes, std::size_t* offset, std::uint16_t* value) {
  if (offset == nullptr || value == nullptr) {
    return false;
  }

  if (*offset + 2U > bytes.size()) {
    return false;
  }

  const std::uint16_t b0 = static_cast<std::uint16_t>(bytes[*offset + 0U]);
  const std::uint16_t b1 = static_cast<std::uint16_t>(bytes[*offset + 1U]) << 8U;
  *value = static_cast<std::uint16_t>(b0 | b1);
  *offset += 2U;
  return true;
}

bool ReadUint32(const std::vector<std::uint8_t>& bytes, std::size_t* offset, std::uint32_t* value) {
  if (offset == nullptr || value == nullptr) {
    return false;
  }

  if (*offset + 4U > bytes.size()) {
    return false;
  }

  const std::uint32_t b0 = static_cast<std::uint32_t>(bytes[*offset + 0U]);
  const std::uint32_t b1 = static_cast<std::uint32_t>(bytes[*offset + 1U]) << 8U;
  const std::uint32_t b2 = static_cast<std::uint32_t>(bytes[*offset + 2U]) << 16U;
  const std::uint32_t b3 = static_cast<std::uint32_t>(bytes[*offset + 3U]) << 24U;
  *value = b0 | b1 | b2 | b3;
  *offset += 4U;
  return true;
}

bool ReadUint64(const std::vector<std::uint8_t>& bytes, std::size_t* offset, std::uint64_t* value) {
  if (offset == nullptr || value == nullptr) {
    return false;
  }

  if (*offset + 8U > bytes.size()) {
    return false;
  }

  const std::uint64_t b0 = static_cast<std::uint64_t>(bytes[*offset + 0U]);
  const std::uint64_t b1 = static_cast<std::uint64_t>(bytes[*offset + 1U]) << 8U;
  const std::uint64_t b2 = static_cast<std::uint64_t>(bytes[*offset + 2U]) << 16U;
  const std::uint64_t b3 = static_cast<std::uint64_t>(bytes[*offset + 3U]) << 24U;
  const std::uint64_t b4 = static_cast<std::uint64_t>(bytes[*offset + 4U]) << 32U;
  const std::uint64_t b5 = static_cast<std::uint64_t>(bytes[*offset + 5U]) << 40U;
  const std::uint64_t b6 = static_cast<std::uint64_t>(bytes[*offset + 6U]) << 48U;
  const std::uint64_t b7 = static_cast<std::uint64_t>(bytes[*offset + 7U]) << 56U;

  *value = b0 | b1 | b2 | b3 | b4 | b5 | b6 | b7;
  *offset += 8U;
  return true;
}

}  // namespace

RowCodecStatus RowCodecStatus::Ok(std::string message) {
  return RowCodecStatus{true, "", std::move(message)};
}

RowCodecStatus RowCodecStatus::Error(std::string code, std::string message) {
  return RowCodecStatus{false, std::move(code), std::move(message)};
}

RowCodecStatus SerializeRow(const std::vector<parser::ColumnDefinition>& columns,
                            const std::vector<parser::ValueLiteral>& values,
                            std::vector<std::uint8_t>* out_bytes) {
  if (out_bytes == nullptr) {
    return RowCodecStatus::Error("E3200", "output row buffer pointer is null");
  }

  if (columns.size() > static_cast<std::size_t>(std::numeric_limits<std::uint16_t>::max())) {
    return RowCodecStatus::Error("E3201", "column count exceeds row codec limit");
  }

  if (columns.size() != values.size()) {
    return RowCodecStatus::Error("E3201", "column/value count mismatch");
  }

  std::vector<std::uint8_t> bytes;
  bytes.reserve(64U);
  WriteUint16(&bytes, static_cast<std::uint16_t>(columns.size()));

  for (std::size_t index = 0U; index < columns.size(); ++index) {
    const parser::ColumnDefinition& column = columns[index];
    const parser::ValueLiteral& value = values[index];

    if (column.type == parser::ColumnType::Integer) {
      if (!std::holds_alternative<std::int64_t>(value.value)) {
        return RowCodecStatus::Error("E3202", "type mismatch at column '" + column.name + "': expected INTEGER");
      }

      WriteUint8(&bytes, kIntegerTag);
      WriteUint64(&bytes, static_cast<std::uint64_t>(std::get<std::int64_t>(value.value)));
      continue;
    }

    if (!std::holds_alternative<std::string>(value.value)) {
      return RowCodecStatus::Error("E3202", "type mismatch at column '" + column.name + "': expected TEXT");
    }

    const std::string& text = std::get<std::string>(value.value);
    if (text.size() > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
      return RowCodecStatus::Error("E3203", "text literal exceeds row codec size limit");
    }

    WriteUint8(&bytes, kTextTag);
    WriteUint32(&bytes, static_cast<std::uint32_t>(text.size()));
    bytes.insert(bytes.end(), reinterpret_cast<const std::uint8_t*>(text.data()),
                 reinterpret_cast<const std::uint8_t*>(text.data()) + text.size());
  }

  *out_bytes = std::move(bytes);
  return RowCodecStatus::Ok("serialized row");
}

RowCodecStatus DeserializeRow(const std::vector<parser::ColumnDefinition>& columns,
                              const std::vector<std::uint8_t>& bytes,
                              std::vector<parser::ValueLiteral>* out_values) {
  if (out_values == nullptr) {
    return RowCodecStatus::Error("E3200", "output row values pointer is null");
  }

  std::size_t offset = 0U;
  std::uint16_t column_count = 0U;
  if (!ReadUint16(bytes, &offset, &column_count)) {
    return RowCodecStatus::Error("E3204", "row payload is truncated");
  }

  if (static_cast<std::size_t>(column_count) != columns.size()) {
    return RowCodecStatus::Error("E3205", "row column count does not match schema");
  }

  std::vector<parser::ValueLiteral> values;
  values.reserve(columns.size());

  for (std::size_t index = 0U; index < columns.size(); ++index) {
    const parser::ColumnDefinition& column = columns[index];

    std::uint8_t tag = 0U;
    if (!ReadUint8(bytes, &offset, &tag)) {
      return RowCodecStatus::Error("E3204", "row payload is truncated");
    }

    if (column.type == parser::ColumnType::Integer) {
      if (tag != kIntegerTag) {
        return RowCodecStatus::Error("E3205", "row value tag does not match schema at column '" + column.name + "'");
      }

      std::uint64_t encoded = 0U;
      if (!ReadUint64(bytes, &offset, &encoded)) {
        return RowCodecStatus::Error("E3204", "row integer payload is truncated");
      }

      values.emplace_back(static_cast<std::int64_t>(encoded));
      continue;
    }

    if (tag != kTextTag) {
      return RowCodecStatus::Error("E3205", "row value tag does not match schema at column '" + column.name + "'");
    }

    std::uint32_t length = 0U;
    if (!ReadUint32(bytes, &offset, &length)) {
      return RowCodecStatus::Error("E3204", "row text length is truncated");
    }

    if (offset + static_cast<std::size_t>(length) > bytes.size()) {
      return RowCodecStatus::Error("E3204", "row text payload is truncated");
    }

    std::string text(reinterpret_cast<const char*>(bytes.data() + offset), static_cast<std::size_t>(length));
    offset += static_cast<std::size_t>(length);
    values.emplace_back(std::move(text));
  }

  if (offset != bytes.size()) {
    return RowCodecStatus::Error("E3206", "row payload contains trailing bytes");
  }

  *out_values = std::move(values);
  return RowCodecStatus::Ok("deserialized row");
}

}  // namespace atlasdb::storage
