#include <iostream>
#include <string>

#include "atlasdb/database.hpp"
#include "atlasdb/version.hpp"

int main() {
  atlasdb::DatabaseEngine engine;

  std::cout << atlasdb::kEngineName << " " << atlasdb::kVersion << "\n";
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

    const atlasdb::Status status = engine.Execute(line);
    if (status.ok) {
      std::cout << "ok: " << status.message << "\n";
    } else {
      std::cout << "error: " << status.message << "\n";
    }
  }

  return 0;
}
