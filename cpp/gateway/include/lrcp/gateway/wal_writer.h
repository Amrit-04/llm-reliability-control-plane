#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <mutex>
#include <string>
#include <string_view>

namespace lrcp::gateway {

class WalWriter {
 public:
  explicit WalWriter(std::filesystem::path directory,
                     std::size_t max_file_bytes = 64U * 1024U * 1024U);
  ~WalWriter();
  WalWriter(const WalWriter&) = delete;
  WalWriter& operator=(const WalWriter&) = delete;
  WalWriter(WalWriter&&) = delete;
  WalWriter& operator=(WalWriter&&) = delete;

  // Writes [little-endian uint32 payload size][little-endian uint32 crc32][payload]
  // and syncs the record before returning true.
  // Rotates to a new segment when current file size + record size exceeds max_file_bytes.
  [[nodiscard]] bool append(std::string_view payload, std::string& error);

  [[nodiscard]] std::filesystem::path current_path() const;

 private:
  void open_new_segment();
  void close_current_handle();

  std::filesystem::path directory_;
  std::size_t max_file_bytes_;
  std::filesystem::path current_path_;
  std::size_t current_file_size_{0};
  mutable std::mutex mutex_;
  int handle_{-1};
};

}  // namespace lrcp::gateway
