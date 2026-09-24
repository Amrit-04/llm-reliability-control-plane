#pragma once

#include <boost/asio.hpp>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>

namespace lrcp::gateway {

struct ServerConfig {
  std::string address{"127.0.0.1"};
  std::uint16_t port{4318};
  std::size_t max_body_bytes{4U * 1024U * 1024U};
  std::filesystem::path wal_directory{"data/wal"};
  std::size_t max_wal_file_bytes{64U * 1024U * 1024U};
};

struct GatewayStats {
  std::uint64_t accepted_requests{};
  std::uint64_t accepted_resource_spans{};
  std::uint64_t accepted_spans{};
  std::uint64_t rejected_requests{};
};

class HttpServer {
 public:
  HttpServer(boost::asio::io_context& io_context, ServerConfig config);
  ~HttpServer();

  HttpServer(const HttpServer&) = delete;
  HttpServer& operator=(const HttpServer&) = delete;

  void start();
  void stop();
  [[nodiscard]] std::uint16_t port() const;
  [[nodiscard]] GatewayStats stats() const;
  [[nodiscard]] std::filesystem::path current_wal_path() const;

 private:
  class SharedState;
  std::shared_ptr<SharedState> state_;
};

}  // namespace lrcp::gateway
