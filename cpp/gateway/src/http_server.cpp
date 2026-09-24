#include "lrcp/gateway/http_server.h"
#include "lrcp/gateway/wal_writer.h"

#include <boost/asio/dispatch.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include <opentelemetry/proto/collector/trace/v1/trace_service.pb.h>

#include <iostream>
#include <optional>
#include <utility>

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = asio::ip::tcp;

namespace lrcp::gateway {
namespace {

constexpr auto kOtlpProtobuf = "application/x-protobuf";

bool is_protobuf_content_type(const beast::string_view value) {
  const auto separator = value.find(';');
  return value.substr(0, separator) == kOtlpProtobuf;
}

template <typename Body>
http::response<http::string_body> error_response(
    const http::request<Body>& request, const http::status status,
    const std::string& message) {
  http::response<http::string_body> response{status, request.version()};
  response.set(http::field::content_type, "text/plain; charset=utf-8");
  response.keep_alive(request.keep_alive());
  response.body() = message;
  response.prepare_payload();
  return response;
}

}  // namespace

class HttpServer::SharedState : public std::enable_shared_from_this<SharedState> {
 public:
  SharedState(asio::io_context& io_context, ServerConfig config)
      : io_context_(io_context),
        config_(std::move(config)),
        acceptor_(io_context),
        wal_writer_(config_.wal_directory, config_.max_wal_file_bytes) {}

  void start() {
    beast::error_code error;
    const auto address = asio::ip::make_address(config_.address, error);
    if (error) {
      throw boost::system::system_error(error, "invalid listen address");
    }
    const tcp::endpoint endpoint{address, config_.port};
    acceptor_.open(endpoint.protocol(), error);
    if (!error) acceptor_.set_option(asio::socket_base::reuse_address(true), error);
    if (!error) acceptor_.bind(endpoint, error);
    if (!error) acceptor_.listen(asio::socket_base::max_listen_connections, error);
    if (error) throw boost::system::system_error(error, "cannot listen");
    accept();
  }

  void stop() {
    beast::error_code ignored;
    acceptor_.close(ignored);
  }

  [[nodiscard]] std::uint16_t port() const {
    beast::error_code error;
    const auto endpoint = acceptor_.local_endpoint(error);
    return error ? 0 : endpoint.port();
  }

  [[nodiscard]] GatewayStats stats() const {
    return {accepted_requests_.load(), accepted_resource_spans_.load(),
            accepted_spans_.load(), rejected_requests_.load()};
  }

  [[nodiscard]] std::filesystem::path current_wal_path() const {
    return wal_writer_.current_path();
  }

 private:
  class Session : public std::enable_shared_from_this<Session> {
   public:
    Session(tcp::socket socket, std::shared_ptr<SharedState> state)
        : socket_(std::move(socket)), state_(std::move(state)) {}

    void run() { read(); }

   private:
    void read() {
      request_ = {};
      parser_.emplace();
      parser_->body_limit(state_->config_.max_body_bytes);
      http::async_read(socket_, buffer_, *parser_,
                       beast::bind_front_handler(&Session::on_read,
                                                 shared_from_this()));
    }

    void on_read(beast::error_code error, std::size_t) {
      if (error == http::error::end_of_stream) return close();
      if (error == http::error::body_limit) {
        ++state_->rejected_requests_;
        return write(error_response(request_, http::status::payload_too_large,
                                    "request body exceeds configured limit\n"));
      }
      if (error) {
        if (error != asio::error::operation_aborted) std::cerr << "read: " << error.message() << '\n';
        return;
      }
      request_ = parser_->release();
      handle_request();
    }

    void handle_request() {
      if (request_.method() == http::verb::get && request_.target() == "/healthz") {
        http::response<http::string_body> response{http::status::ok, request_.version()};
        response.set(http::field::content_type, "text/plain; charset=utf-8");
        response.keep_alive(request_.keep_alive());
        response.body() = "ok\n";
        response.prepare_payload();
        return write(std::move(response));
      }
      if (request_.method() != http::verb::post || request_.target() != "/v1/traces") {
        ++state_->rejected_requests_;
        return write(error_response(request_, http::status::not_found, "only POST /v1/traces is supported\n"));
      }
      if (!is_protobuf_content_type(request_[http::field::content_type])) {
        ++state_->rejected_requests_;
        return write(error_response(request_, http::status::unsupported_media_type,
                                    "expected Content-Type: application/x-protobuf\n"));
      }

      opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest export_request;
      if (!export_request.ParseFromString(request_.body())) {
        ++state_->rejected_requests_;
        return write(error_response(request_, http::status::bad_request, "malformed OTLP protobuf\n"));
      }

      std::string wal_error;
      if (!state_->wal_writer_.append(request_.body(), wal_error)) {
        ++state_->rejected_requests_;
        std::cerr << "WAL append failed: " << wal_error << '\n';
        return write(error_response(request_, http::status::service_unavailable,
                                    "telemetry storage is unavailable\n"));
      }

      std::uint64_t spans = 0;
      for (const auto& resource_spans : export_request.resource_spans()) {
        for (const auto& scope_spans : resource_spans.scope_spans()) spans += scope_spans.spans_size();
      }
      ++state_->accepted_requests_;
      state_->accepted_resource_spans_ += export_request.resource_spans_size();
      state_->accepted_spans_ += spans;

      opentelemetry::proto::collector::trace::v1::ExportTraceServiceResponse export_response;
      std::string body;
      if (!export_response.SerializeToString(&body)) {
        ++state_->rejected_requests_;
        return write(error_response(request_, http::status::internal_server_error,
                                    "could not serialize OTLP response\n"));
      }
      http::response<http::string_body> response{http::status::ok, request_.version()};
      response.set(http::field::content_type, kOtlpProtobuf);
      response.keep_alive(request_.keep_alive());
      response.body() = std::move(body);
      response.prepare_payload();
      write(std::move(response));
    }

    void write(http::response<http::string_body> response) {
      const auto close_after_write = response.need_eof();
      response_ = std::make_shared<http::response<http::string_body>>(std::move(response));
      http::async_write(socket_, *response_,
                        [self = shared_from_this(), close_after_write](beast::error_code error, std::size_t) {
                          self->response_.reset();
                          if (error) return;
                          if (close_after_write) return self->close();
                          self->read();
                        });
    }

    void close() {
      beast::error_code ignored;
      socket_.shutdown(tcp::socket::shutdown_send, ignored);
    }

    tcp::socket socket_;
    beast::flat_buffer buffer_;
    std::optional<http::request_parser<http::string_body>> parser_;
    http::request<http::string_body> request_;
    std::shared_ptr<http::response<http::string_body>> response_;
    std::shared_ptr<SharedState> state_;
  };

  void accept() {
    acceptor_.async_accept(asio::make_strand(io_context_),
                           [self = shared_from_this()](beast::error_code error, tcp::socket socket) {
                             if (!error) std::make_shared<Session>(std::move(socket), self)->run();
                             if (error != asio::error::operation_aborted) self->accept();
                           });
  }

  asio::io_context& io_context_;
  ServerConfig config_;
  tcp::acceptor acceptor_;
  WalWriter wal_writer_;
  std::atomic_uint64_t accepted_requests_{0};
  std::atomic_uint64_t accepted_resource_spans_{0};
  std::atomic_uint64_t accepted_spans_{0};
  std::atomic_uint64_t rejected_requests_{0};
};

HttpServer::HttpServer(asio::io_context& io_context, ServerConfig config)
    : state_(std::make_shared<SharedState>(io_context, std::move(config))) {}
HttpServer::~HttpServer() = default;
void HttpServer::start() { state_->start(); }
void HttpServer::stop() { state_->stop(); }
std::uint16_t HttpServer::port() const { return state_->port(); }
GatewayStats HttpServer::stats() const { return state_->stats(); }
std::filesystem::path HttpServer::current_wal_path() const { return state_->current_wal_path(); }

}  // namespace lrcp::gateway
