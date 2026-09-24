#include "lrcp/gateway/http_server.h"

#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <gtest/gtest.h>

#include <opentelemetry/proto/collector/trace/v1/trace_service.pb.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <system_error>
#include <string>
#include <string_view>
#include <thread>

namespace asio = boost::asio;
namespace http = boost::beast::http;
using tcp = asio::ip::tcp;

namespace {

std::uint32_t ieee_crc32(std::string_view payload) {
  std::uint32_t crc = 0xFFFFFFFFU;
  for (const unsigned char character : payload) {
    crc ^= character;
    for (int bit = 0; bit < 8; ++bit) {
      crc = (crc >> 1U) ^ (0xEDB88320U & static_cast<std::uint32_t>(-static_cast<int>(crc & 1U)));
    }
  }
  return ~crc;
}

TEST(Crc32Compatibility, MatchesPythonBinAsciiVector) {
  // binascii.crc32(b"123456789") & 0xFFFFFFFF == 0xCBF43926
  EXPECT_EQ(ieee_crc32("123456789"), 0xCBF43926U);
}

class GatewayTest : public ::testing::Test {
 protected:
  GatewayTest()
      : wal_directory_(std::filesystem::temp_directory_path() /
                       ("lrcp-gateway-test-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()))) {}

  void SetUp() override {
    server_ = std::make_unique<lrcp::gateway::HttpServer>(
        io_context_, lrcp::gateway::ServerConfig{"127.0.0.1", 0, 256, wal_directory_});
    server_->start();
    worker_ = std::thread([this] { io_context_.run(); });
  }

  void TearDown() override {
    if (server_) server_->stop();
    io_context_.stop();
    if (worker_.joinable()) worker_.join();
    // Close the WAL handle before deleting the temp directory (required on Windows).
    server_.reset();
    std::error_code ignored;
    std::filesystem::remove_all(wal_directory_, ignored);
  }

  http::response<http::string_body> request(http::request<http::string_body> message) {
    asio::io_context client_context;
    tcp::socket socket{client_context};
        socket.connect({asio::ip::make_address("127.0.0.1"), server_->port()});
    http::write(socket, message);
    boost::beast::flat_buffer buffer;
    http::response<http::string_body> response;
    http::read(socket, buffer, response);
    return response;
  }

  asio::io_context io_context_;
  std::filesystem::path wal_directory_;
  std::unique_ptr<lrcp::gateway::HttpServer> server_;
  std::thread worker_;
};

TEST_F(GatewayTest, HealthEndpointReturnsOk) {
  http::request<http::string_body> message{http::verb::get, "/healthz", 11};
  const auto response = request(std::move(message));
  EXPECT_EQ(response.result(), http::status::ok);
  EXPECT_EQ(response.body(), "ok\n");
}

TEST_F(GatewayTest, AcceptsBinaryOtlpTraceRequest) {
  opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest export_request;
  export_request.add_resource_spans()->add_scope_spans()->add_spans();
  std::string payload;
  ASSERT_TRUE(export_request.SerializeToString(&payload));
  http::request<http::string_body> message{http::verb::post, "/v1/traces", 11};
  message.set(http::field::content_type, "application/x-protobuf");
  message.body() = payload;
  message.prepare_payload();
  const auto response = request(std::move(message));
  EXPECT_EQ(response.result(), http::status::ok);
  EXPECT_EQ(server_->stats().accepted_requests, 1);
  EXPECT_EQ(server_->stats().accepted_spans, 1);

  opentelemetry::proto::collector::trace::v1::ExportTraceServiceResponse export_response;
  EXPECT_TRUE(export_response.ParseFromString(response.body()));

  const auto wal_path = server_->current_wal_path();
  ASSERT_TRUE(std::filesystem::exists(wal_path));
  EXPECT_EQ(std::filesystem::file_size(wal_path), 8 + payload.size());

  std::ifstream wal(wal_path, std::ios::binary);
  ASSERT_TRUE(wal);
  std::uint32_t length = 0;
  std::uint32_t checksum = 0;
  wal.read(reinterpret_cast<char*>(&length), sizeof(length));
  wal.read(reinterpret_cast<char*>(&checksum), sizeof(checksum));
  std::string stored(payload.size(), '\0');
  wal.read(stored.data(), static_cast<std::streamsize>(payload.size()));
  EXPECT_EQ(length, payload.size());
  EXPECT_EQ(stored, payload);
  EXPECT_EQ(checksum, ieee_crc32(payload));
}

TEST_F(GatewayTest, RejectsWrongContentType) {
  http::request<http::string_body> message{http::verb::post, "/v1/traces", 11};
  message.set(http::field::content_type, "application/json");
  message.body() = "{}";
  message.prepare_payload();
  EXPECT_EQ(request(std::move(message)).result(), http::status::unsupported_media_type);
}

TEST_F(GatewayTest, RejectsMalformedProtobuf) {
  http::request<http::string_body> message{http::verb::post, "/v1/traces", 11};
  message.set(http::field::content_type, "application/x-protobuf");
  message.body() = "not protobuf";
  message.prepare_payload();
  EXPECT_EQ(request(std::move(message)).result(), http::status::bad_request);
}

TEST_F(GatewayTest, RejectsOversizePayload) {
  http::request<http::string_body> message{http::verb::post, "/v1/traces", 11};
  message.set(http::field::content_type, "application/x-protobuf");
  message.body() = std::string(257, 'x');
  message.prepare_payload();
  EXPECT_EQ(request(std::move(message)).result(), http::status::payload_too_large);
}

TEST_F(GatewayTest, RotatesWalWhenMaxFileBytesExceeded) {
  // Configure with very small max file bytes so rotation happens quickly.
  // Record: 8-byte header + payload.
  // max_file_bytes of 50 means after writing a ~35-byte record, the next record won't fit and rotates.

  if (server_) server_->stop();
  io_context_.stop();
  if (worker_.joinable()) worker_.join();

  server_.reset();
  io_context_.restart();

  server_ = std::make_unique<lrcp::gateway::HttpServer>(
      io_context_,
      lrcp::gateway::ServerConfig{"127.0.0.1", 0, 256, wal_directory_, 50 /* max 50 bytes per segment */});
  server_->start();
  worker_ = std::thread([this] { io_context_.run(); });

  opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest export_request1;
  auto* span1 = export_request1.add_resource_spans()->add_scope_spans()->add_spans();
  span1->set_name("first_operation");
  std::string payload1;
  ASSERT_TRUE(export_request1.SerializeToString(&payload1));

  http::request<http::string_body> message1{http::verb::post, "/v1/traces", 11};
  message1.set(http::field::content_type, "application/x-protobuf");
  message1.body() = payload1;
  message1.prepare_payload();
  const auto response1 = request(message1);
  EXPECT_EQ(response1.result(), http::status::ok);

  // First WAL segment path
  const auto wal_path_1 = server_->current_wal_path();
  ASSERT_TRUE(std::filesystem::exists(wal_path_1));
  EXPECT_EQ(std::filesystem::file_size(wal_path_1), 8 + payload1.size());

  // Send another record to trigger rotation (total size would exceed 50 bytes)
  opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest export_request2;
  auto* span2 = export_request2.add_resource_spans()->add_scope_spans()->add_spans();
  span2->set_name("second_operation");
  std::string payload2;
  ASSERT_TRUE(export_request2.SerializeToString(&payload2));

  http::request<http::string_body> message2{http::verb::post, "/v1/traces", 11};
  message2.set(http::field::content_type, "application/x-protobuf");
  message2.body() = payload2;
  message2.prepare_payload();
  const auto response2 = request(message2);
  EXPECT_EQ(response2.result(), http::status::ok);

  // Second WAL segment path should be different
  const auto wal_path_2 = server_->current_wal_path();
  EXPECT_NE(wal_path_1, wal_path_2);

  // Both files should exist
  ASSERT_TRUE(std::filesystem::exists(wal_path_1));
  ASSERT_TRUE(std::filesystem::exists(wal_path_2));

  // First segment should contain only the first record
  EXPECT_EQ(std::filesystem::file_size(wal_path_1), 8 + payload1.size());

  // Second segment should contain only the second record
  EXPECT_EQ(std::filesystem::file_size(wal_path_2), 8 + payload2.size());

  // Verify both files are valid WAL segments
  std::ifstream wal1(wal_path_1, std::ios::binary);
  std::uint32_t length1 = 0, checksum1 = 0;
  wal1.read(reinterpret_cast<char*>(&length1), sizeof(length1));
  wal1.read(reinterpret_cast<char*>(&checksum1), sizeof(checksum1));
  std::string stored1(length1, '\0');
  wal1.read(stored1.data(), static_cast<std::streamsize>(length1));
  EXPECT_EQ(length1, payload1.size());
  EXPECT_EQ(stored1, payload1);
  EXPECT_EQ(checksum1, ieee_crc32(payload1));

  std::ifstream wal2(wal_path_2, std::ios::binary);
  std::uint32_t length2 = 0, checksum2 = 0;
  wal2.read(reinterpret_cast<char*>(&length2), sizeof(length2));
  wal2.read(reinterpret_cast<char*>(&checksum2), sizeof(checksum2));
  std::string stored2(length2, '\0');
  wal2.read(stored2.data(), static_cast<std::streamsize>(length2));
  EXPECT_EQ(length2, payload2.size());
  EXPECT_EQ(stored2, payload2);
  EXPECT_EQ(checksum2, ieee_crc32(payload2));
}

}  // namespace
