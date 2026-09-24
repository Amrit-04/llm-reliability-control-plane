cmake_minimum_required(VERSION 3.25)

project(llm_reliability_control_plane VERSION 0.1.0 LANGUAGES CXX)

option(LRCP_BUILD_TESTS "Build gateway tests" ON)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(POLICY CMP0167)
  cmake_policy(SET CMP0167 NEW)
endif()

if(WIN32)
  add_compile_definitions(_WIN32_WINNT=0x0A00)
endif()

include(FetchContent)

# Pinned source revisions make the generated OTLP interface reproducible.  This
# project deliberately fetches source at configure time instead of vendoring it.
FetchContent_Declare(
  opentelemetry_proto
  GIT_REPOSITORY https://github.com/open-telemetry/opentelemetry-proto.git
  # v1.9.0; pin the immutable commit rather than a mutable branch or tag name.
  GIT_TAG a8951735f7801e8adfaec5c0ace9262771cfec6e
  GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(opentelemetry_proto)

find_package(Boost 1.82 REQUIRED COMPONENTS system)
find_package(Protobuf REQUIRED)
find_package(Threads REQUIRED)

file(GLOB_RECURSE LRCP_OTEL_PROTO_FILES CONFIGURE_DEPENDS
  "${opentelemetry_proto_SOURCE_DIR}/opentelemetry/proto/*.proto")

add_library(lrcp_otlp_proto)
target_include_directories(lrcp_otlp_proto PUBLIC "${CMAKE_CURRENT_BINARY_DIR}")
target_link_libraries(lrcp_otlp_proto PUBLIC protobuf::libprotobuf)
protobuf_generate(
  TARGET lrcp_otlp_proto
  LANGUAGE cpp
  PROTOS ${LRCP_OTEL_PROTO_FILES}
  IMPORT_DIRS "${opentelemetry_proto_SOURCE_DIR}"
)

add_library(lrcp_gateway_lib
  cpp/gateway/src/http_server.cpp
  cpp/gateway/src/wal_writer.cpp
)
target_include_directories(lrcp_gateway_lib PUBLIC cpp/gateway/include)
target_link_libraries(lrcp_gateway_lib
  PUBLIC Boost::system Threads::Threads lrcp_otlp_proto
)
target_compile_features(lrcp_gateway_lib PUBLIC cxx_std_20)

add_executable(lrcp-gateway cpp/gateway/src/main.cpp)
target_link_libraries(lrcp-gateway PRIVATE lrcp_gateway_lib)

if(LRCP_BUILD_TESTS)
  enable_testing()
  find_package(GTest REQUIRED)
  add_executable(gateway_tests cpp/gateway/tests/gateway_tests.cpp)
  target_link_libraries(gateway_tests PRIVATE lrcp_gateway_lib GTest::gtest_main)
  include(GoogleTest)
  gtest_discover_tests(gateway_tests)
endif()
