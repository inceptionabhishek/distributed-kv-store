FROM ubuntu:24.04 AS build
RUN apt-get update && apt-get install -y --no-install-recommends build-essential cmake pkg-config libgrpc++-dev protobuf-compiler-grpc libprotobuf-dev protobuf-compiler libgtest-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /source
COPY . .
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build --parallel 2 && ctest --test-dir build --output-on-failure --timeout 30

FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends libgrpc++1.51t64 libprotobuf32t64 && rm -rf /var/lib/apt/lists/*
COPY --from=build /source/build/kv_server /source/build/kv_router /source/build/kvctl /usr/local/bin/
RUN useradd --create-home kv && mkdir /data && chown kv:kv /data
USER kv
WORKDIR /data
CMD ["kv_router", "--serve", "50050", "--data", "/data/router"]
