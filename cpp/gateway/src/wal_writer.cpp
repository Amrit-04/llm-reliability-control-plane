#include "lrcp/gateway/wal_writer.h"

#include <array>
#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <system_error>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#include <sys/stat.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace lrcp::gateway {
namespace {

std::uint32_t crc32(const std::string_view payload) {
  std::uint32_t crc = 0xFFFFFFFFU;
  for (const auto character : payload) {
    crc ^= static_cast<unsigned char>(character);
    for (int bit = 0; bit < 8; ++bit) crc = (crc >> 1U) ^ (0xEDB88320U & (-(crc & 1U)));
  }
  return ~crc;
}

void write_little_endian(std::array<char, 8>& buffer, const std::uint32_t size,
                         const std::uint32_t checksum) {
  for (int byte = 0; byte < 4; ++byte) {
    buffer[byte] = static_cast<char>((size >> (byte * 8)) & 0xFFU);
    buffer[byte + 4] = static_cast<char>((checksum >> (byte * 8)) & 0xFFU);
  }
}

bool write_all(const int handle, const char* data, std::size_t remaining) {
  while (remaining > 0) {
#ifdef _WIN32
    const auto written = _write(handle, data, static_cast<unsigned int>(remaining));
#else
    const auto written = write(handle, data, remaining);
#endif
    if (written <= 0) return false;
    data += written;
    remaining -= static_cast<std::size_t>(written);
  }
  return true;
}

}  // namespace

WalWriter::WalWriter(std::filesystem::path directory, std::size_t max_file_bytes)
    : directory_(std::move(directory)), max_file_bytes_(max_file_bytes) {
  std::filesystem::create_directories(directory_);
  open_new_segment();
}

WalWriter::~WalWriter() {
  close_current_handle();
}

void WalWriter::close_current_handle() {
  if (handle_ < 0) return;
#ifdef _WIN32
  _close(handle_);
#else
  close(handle_);
#endif
  handle_ = -1;
}

void WalWriter::open_new_segment() {
  close_current_handle();
  const auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
  static std::atomic<std::uint64_t> sequence{0};
  current_path_ = directory_ / ("wal-" + std::to_string(now) + "-" +
                                std::to_string(sequence.fetch_add(1, std::memory_order_relaxed)) + ".wal");
#ifdef _WIN32
  handle_ = _open(current_path_.string().c_str(), _O_BINARY | _O_CREAT | _O_APPEND | _O_WRONLY,
                  _S_IREAD | _S_IWRITE);
#else
  handle_ = open(current_path_.c_str(), O_CREAT | O_APPEND | O_WRONLY, 0640);
#endif
  if (handle_ < 0) throw std::system_error(errno, std::generic_category(), "open WAL segment " + current_path_.string());
  current_file_size_ = 0;
}

std::filesystem::path WalWriter::current_path() const {
  std::scoped_lock lock(mutex_);
  return current_path_;
}

bool WalWriter::append(const std::string_view payload, std::string& error) {
  if (payload.size() > UINT32_MAX) { error = "payload exceeds WAL record limit"; return false; }
  const std::size_t record_size = 8 + payload.size();
  std::scoped_lock lock(mutex_);

  if (handle_ < 0 || (current_file_size_ > 0 && current_file_size_ + record_size > max_file_bytes_)) {
    try {
      open_new_segment();
    } catch (const std::exception& ex) {
      error = ex.what();
      return false;
    }
  }

  std::array<char, 8> header{};
  write_little_endian(header, static_cast<std::uint32_t>(payload.size()), crc32(payload));
  if (!write_all(handle_, header.data(), header.size()) || !write_all(handle_, payload.data(), payload.size())) {
    error = std::strerror(errno);
    return false;
  }
#ifdef _WIN32
  if (_commit(handle_) != 0) {
#else
  if (fsync(handle_) != 0) {
#endif
    error = std::strerror(errno);
    return false;
  }
  current_file_size_ += record_size;
  return true;
}

}  // namespace lrcp::gateway
