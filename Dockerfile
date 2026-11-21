FROM gcc:13-bookworm AS builder

WORKDIR /src
COPY parquet_writer.hpp main.cpp /src/
RUN g++ -O3 -march=x86-64-v3 -flto -std=c++20 -pthread -static-libstdc++ -static-libgcc /src/main.cpp -o /src/pipeline \
    && strip /src/pipeline

FROM debian:bookworm-slim

RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=builder /src/pipeline /app/pipeline
RUN chmod 755 /app/pipeline

WORKDIR /app
CMD ["/app/pipeline"]
