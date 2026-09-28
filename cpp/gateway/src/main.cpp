#include "lrcp/gateway/http_server.h"

#include <boost/asio.hpp>

#include <csignal>
#include <cstdlib>
#include <exception>
#include <limits>
#include <stdexcept>
#include <iostream>
#include <string_view>

namespace {

void usage() {
  std::cerr << "usage: lrcp-gateway [--address ADDRESS] [--port PORT] [--max-body-bytes BYTES] [--max-wal-file-bytes BYTES] [--wal-dir PATH] [--auth-token TOKEN]\n";
}

}  // namespace

int main(int argc, char* argv[]) {
  lrcp::gateway::ServerConfig config;
  for (int index = 1; index < argc; index += 2) {
    const std::string_view option{argv[index]};
    if (option == "--help") { usage(); return EXIT_SUCCESS; }
    if (index + 1 >= argc) { usage(); return EXIT_FAILURE; }
    try {
      if (option == "--address") config.address = argv[index + 1];
      else if (option == "--port") {
        const auto port = std::stoul(argv[index + 1]);
        if (port > std::numeric_limits<std::uint16_t>::max()) throw std::out_of_range("port");
        config.port = static_cast<std::uint16_t>(port);
      }
      else if (option == "--max-body-bytes") config.max_body_bytes = std::stoull(argv[index + 1]);
      else if (option == "--max-wal-file-bytes") config.max_wal_file_bytes = std::stoull(argv[index + 1]);
      else if (option == "--wal-dir") config.wal_directory = argv[index + 1];
      else if (option == "--auth-token") config.auth_token = argv[index + 1];
      else { usage(); return EXIT_FAILURE; }
    } catch (const std::exception&) {
      usage(); return EXIT_FAILURE;
    }
  }
  if (config.max_body_bytes == 0) { std::cerr << "max-body-bytes must be positive\n"; return EXIT_FAILURE; }
  if (config.max_wal_file_bytes == 0) { std::cerr << "max-wal-file-bytes must be positive\n"; return EXIT_FAILURE; }

  try {
    const auto hardware_concurrency = std::thread::hardware_concurrency();
    const auto num_threads = hardware_concurrency > 0 ? hardware_concurrency : 4;
    boost::asio::io_context io_context{static_cast<int>(num_threads)};
    lrcp::gateway::HttpServer server{io_context, config};
    server.start();
    std::cout << "listening on " << config.address << ':' << server.port() << std::endl;
    boost::asio::signal_set signals{io_context, SIGINT, SIGTERM};
    signals.async_wait([&server, &io_context](const boost::system::error_code&, int) {
      server.stop();
      io_context.stop();
    });

    std::vector<std::thread> workers;
    for (unsigned int i = 1; i < num_threads; ++i) {
      workers.emplace_back([&io_context] { io_context.run(); });
    }
    io_context.run();
    for (auto& worker : workers) {
      if (worker.joinable()) worker.join();
    }
  } catch (const std::exception& error) {
    std::cerr << "fatal: " << error.what() << '\n';
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
