FROM ubuntu:24.04 AS build
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake git libboost-dev libgtest-dev libprotobuf-dev ninja-build protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cmake -S . -B build -G Ninja -DLRCP_BUILD_TESTS=OFF \
    && cmake --build build --target lrcp-gateway

FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl libboost-system1.83 libprotobuf32 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/build/lrcp-gateway /usr/local/bin/lrcp-gateway
COPY cpp/gateway/docker-entrypoint.sh /usr/local/bin/lrcp-gateway-entrypoint
RUN chmod +x /usr/local/bin/lrcp-gateway-entrypoint \
    && mkdir -p /data/wal
# Compose bind-mounts /data as root. The entrypoint creates the WAL directory
# before exec; run the process as root in this experimental image so the volume
# is writable. Tighten this with a dedicated user and an init container in k8s.
EXPOSE 4318
ENTRYPOINT ["/usr/local/bin/lrcp-gateway-entrypoint"]
CMD ["--address", "0.0.0.0", "--port", "4318", "--wal-dir", "/data/wal"]
