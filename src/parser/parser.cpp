#include "atlasdb/parser/parser.hpp"

#include <charconv>
#include <cctype>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace atlasdb::parser {
namespace {

enum class TokenKind {
  Identifier,
  Number,
  StringLiteral,
  LeftParen,
  RightParen,
  Comma,
  Semicolon,
  End,
};

struct Token {
  TokenKind kind;
  std::string lexeme;
  std::size_t position;
};

char UpperAscii(char value) {
  return static_cast<char>(std::toupper(static_cast<unsigned char>(value)));
}

bool IsIdentifierStart(char value) {
  return std::isalpha(static_cast<unsigned char>(value)) != 0 || value == '_';
}

bool IsIdentifierBody(char value) {
  return std::isalnum(static_cast<unsigned char>(value)) != 0 || value == '_';
}

bool EqualsIgnoreCase(std::string_view lhs, std::string_view rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }

  for (std::size_t idx = 0; idx < lhs.size(); ++idx) {
    if (UpperAscii(lhs[idx]) != UpperAscii(rhs[idx])) {
      return false;
    }
  }

  return true;
}

ParseResult MakeParseError(std::string code, std::string message, std::size_t position) {
  ParseResult result;
  result.ok = false;
  result.error = ParseError{std::move(code), std::move(message), position};
  return result;
}

class Lexer {
 public:
  explicit Lexer(std::string_view source) : source_(source) {}

  ParseResult Tokenize(std::vector<Token>* out_tokens) {
    while (!IsAtEnd()) {
      SkipWhitespace();
      if (IsAtEnd()) {
        break;
      }

      const char current = source_[cursor_];

      if (IsIdentifierStart(current)) {
        out_tokens->push_back(ReadIdentifier());
        continue;
      }

      if (std::isdigit(static_cast<unsigned char>(current)) != 0) {
        out_tokens->push_back(ReadNumber());
        continue;
      }

      if (current == '\'') {
        ParseResult string_result = ReadString(out_tokens);
        if (!string_result.ok) {
          return string_result;
        }
        continue;
      }

      switch (current) {
        case '(':
          out_tokens->push_back(MakeToken(TokenKind::LeftParen, "(", cursor_));
          ++cursor_;
          break;
        case ')':
          out_tokens->push_back(MakeToken(TokenKind::RightParen, ")", cursor_));
          ++cursor_;
          break;
        case ',':
          out_tokens->push_back(MakeToken(TokenKind::Comma, ",", cursor_));
          ++cursor_;
          break;
        case ';':
          out_tokens->push_back(MakeToken(TokenKind::Semicolon, ";", cursor_));
          ++cursor_;
          break;
        default:
          return MakeParseError("E1100", "unexpected character in statement", cursor_);
      }
    }

    out_tokens->push_back(MakeToken(TokenKind::End, "", source_.size()));
    ParseResult ok;
    ok.ok = true;
    return ok;
  }

 private:
  bool IsAtEnd() const {
    return cursor_ >= source_.size();
  }

  void SkipWhitespace() {
    while (!IsAtEnd() && std::isspace(static_cast<unsigned char>(source_[cursor_])) != 0) {
      ++cursor_;
    }
  }

  Token MakeToken(TokenKind kind, std::string lexeme, std::size_t position) {
    return Token{kind, std::move(lexeme), position};
  }

  Token ReadIdentifier() {
    const std::size_t start = cursor_;
    ++cursor_;
    while (!IsAtEnd() && IsIdentifierBody(source_[cursor_])) {
      ++cursor_;
    }

    return MakeToken(TokenKind::Identifier, std::string(source_.substr(start, cursor_ - start)), start);
  }

  Token ReadNumber() {
    const std::size_t start = cursor_;
    ++cursor_;
    while (!IsAtEnd() && std::isdigit(static_cast<unsigned char>(source_[cursor_])) != 0) {
      ++cursor_;
    }

    return MakeToken(TokenKind::Number, std::string(source_.substr(start, cursor_ - start)), start);
  }

  ParseResult ReadString(std::vector<Token>* out_tokens) {
    const std::size_t start = cursor_;
    ++cursor_;

    std::string value;
    while (!IsAtEnd()) {
      const char current = source_[cursor_];
      if (current == '\'') {
        if (cursor_ + 1U < source_.size() && source_[cursor_ + 1U] == '\'') {
          value.push_back('\'');
          cursor_ += 2U;
          continue;
        }

        ++cursor_;
        out_tokens->push_back(MakeToken(TokenKind::StringLiteral, value, start));

        ParseResult ok;
        ok.ok = true;
        return ok;
      }

      value.push_back(current);
      ++cursor_;
    }

    return MakeParseError("E1101", "unterminated string literal", start);
  }

  std::string_view source_;
  std::size_t cursor_{0};
};

class StatementParser {
 public:
  explicit StatementParser(std::vector<Token> tokens) : tokens_(std::move(tokens)) {}

  ParseResult ParseStatement() {
    if (MatchKeyword("CREATE")) {
      return ParseCreateTable();
    }

    if (MatchKeyword("INSERT")) {
      return ParseInsert();
    }

    return MakeParseError("E1200", "unsupported statement; expected CREATE TABLE or INSERT INTO",
                          Current().position);
  }

 private:
  const Token& Current() const {
    return tokens_[cursor_];
  }

  bool IsAtEnd() const {
    return Current().kind == TokenKind::End;
  }

  const Token& Advance() {
    if (!IsAtEnd()) {
      ++cursor_;
    }
    return tokens_[cursor_ - 1U];
  }

  bool Check(TokenKind kind) const {
    return Current().kind == kind;
  }

  bool Match(TokenKind kind) {
    if (!Check(kind)) {
      return false;
    }
    Advance();
    return true;
  }

  bool CheckKeyword(std::string_view keyword) const {
    return Check(TokenKind::Identifier) && EqualsIgnoreCase(Current().lexeme, keyword);
  }

  bool MatchKeyword(std::string_view keyword) {
    if (!CheckKeyword(keyword)) {
      return false;
    }
    Advance();
    return true;
  }

  ParseResult ParseCreateTable() {
    if (!MatchKeyword("TABLE")) {
      return MakeParseError("E1201", "expected TABLE keyword after CREATE", Current().position);
    }

    if (!Check(TokenKind::Identifier)) {
      return MakeParseError("E1202", "expected table name identifier", Current().position);
    }
    const std::string table_name = Advance().lexeme;

    if (!Match(TokenKind::LeftParen)) {
      return MakeParseError("E1203", "expected '(' after table name", Current().position);
    }

    std::vector<ColumnDefinition> columns;
    bool has_primary_key = false;

    while (true) {
      if (!Check(TokenKind::Identifier)) {
        return MakeParseError("E1204", "expected column name identifier", Current().position);
      }
      const std::string column_name = Advance().lexeme;

      if (!Check(TokenKind::Identifier)) {
        return MakeParseError("E1205", "expected column type", Current().position);
      }
      const Token type_token = Advance();

      ColumnType column_type = ColumnType::Integer;
      if (EqualsIgnoreCase(type_token.lexeme, "INT") || EqualsIgnoreCase(type_token.lexeme, "INTEGER")) {
        column_type = ColumnType::Integer;
      } else if (EqualsIgnoreCase(type_token.lexeme, "TEXT")) {
        column_type = ColumnType::Text;
      } else {
        return MakeParseError("E1206", "invalid column type '" + type_token.lexeme + "'", type_token.position);
      }

      bool primary_key = false;
      if (MatchKeyword("PRIMARY")) {
        if (!MatchKeyword("KEY")) {
          return MakeParseError("E1207", "expected KEY after PRIMARY", Current().position);
        }
        if (has_primary_key) {
          return MakeParseError("E1210", "multiple PRIMARY KEY columns are not supported",
                                Current().position);
        }
        has_primary_key = true;
        primary_key = true;
      }

      columns.push_back(ColumnDefinition{column_name, column_type, primary_key});

      if (Match(TokenKind::Comma)) {
        continue;
      }
      break;
    }

    if (!Match(TokenKind::RightParen)) {
      return MakeParseError("E1208", "expected ')' after column definitions", Current().position);
    }

    Match(TokenKind::Semicolon);

    if (!IsAtEnd()) {
      return MakeParseError("E1209", "unexpected token after CREATE TABLE statement", Current().position);
    }

    ParseResult result;
    result.ok = true;
    result.statement = CreateTableStatement{table_name, columns};
    return result;
  }

  ParseResult ParseInsert() {
    if (!MatchKeyword("INTO")) {
      return MakeParseError("E1301", "expected INTO keyword after INSERT", Current().position);
    }

    if (!Check(TokenKind::Identifier)) {
      return MakeParseError("E1302", "expected table name identifier", Current().position);
    }
    const std::string table_name = Advance().lexeme;

    if (!MatchKeyword("VALUES")) {
      return MakeParseError("E1303", "expected VALUES keyword after table name", Current().position);
    }

    if (!Match(TokenKind::LeftParen)) {
      return MakeParseError("E1304", "expected '(' before values list", Current().position);
    }

    std::vector<ValueLiteral> values;
    while (true) {
      if (Check(TokenKind::Number)) {
        const Token number_token = Advance();

        std::int64_t value = 0;
        const char* begin = number_token.lexeme.data();
        const char* end = begin + number_token.lexeme.size();
        const std::from_chars_result conversion = std::from_chars(begin, end, value);
        if (conversion.ec != std::errc{} || conversion.ptr != end) {
          return MakeParseError("E1305", "invalid integer literal", number_token.position);
        }

        values.push_back(ValueLiteral{value});
      } else if (Check(TokenKind::StringLiteral)) {
        values.push_back(ValueLiteral{Advance().lexeme});
      } else {
        return MakeParseError("E1306", "expected literal value", Current().position);
      }

      if (Match(TokenKind::Comma)) {
        continue;
      }
      break;
    }

    if (!Match(TokenKind::RightParen)) {
      return MakeParseError("E1307", "expected ')' after values list", Current().position);
    }

    Match(TokenKind::Semicolon);

    if (!IsAtEnd()) {
      return MakeParseError("E1308", "unexpected token after INSERT statement", Current().position);
    }

    ParseResult result;
    result.ok = true;
    result.statement = InsertStatement{table_name, values};
    return result;
  }

  std::vector<Token> tokens_;
  std::size_t cursor_{0};
};

}  // namespace

ParseResult ParseSql(std::string_view sql) {
  std::vector<Token> tokens;
  Lexer lexer(sql);
  ParseResult token_result = lexer.Tokenize(&tokens);
  if (!token_result.ok) {
    return token_result;
  }

  StatementParser parser(std::move(tokens));
  return parser.ParseStatement();
}

std::string StatementTypeName(const Statement& statement) {
  if (std::holds_alternative<CreateTableStatement>(statement)) {
    return "CREATE TABLE";
  }
  return "INSERT";
}

}  // namespace atlasdb::parser
