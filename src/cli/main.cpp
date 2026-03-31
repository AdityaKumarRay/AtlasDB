#include <iostream>
#include <memory>
#include <string>

#include "atlasdb/database.hpp"
#include "atlasdb/version.hpp"

int main(int argc, char* argv[]) {
  if (argc > 2) {
    std::cerr << "Usage: atlasdb_cli [database_file]\n";
    return 1;
  }

  std::unique_ptr<atlasdb::DatabaseEngine> engine;
  if (argc == 2) {
    engine = std::make_unique<atlasdb::DatabaseEngine>(std::string(argv[1]));
  } else {
    engine = std::make_unique<atlasdb::DatabaseEngine>();
  }

  std::cout << atlasdb::kEngineName << " " << atlasdb::kVersion << "\n";
  if (argc == 2) {
    std::cout << "Using database file: " << argv[1] << "\n";
  }
  std::cout << "Enter SQL-like statements. Use .exit to quit.\n";

  std::string line;
  while (true) {
    std::cout << "atlasdb> ";
    if (!std::getline(std::cin, line)) {
      std::cout << "\n";
      break;
    }

    if (line == ".exit") {
      break;
    }

    const atlasdb::Status status = engine->Execute(line);
    if (status.ok) {
      std::cout << "ok: " << status.message << "\n";
    } else {
      std::cout << "error: " << status.message << "\n";
    }
  }

  return 0;
}
