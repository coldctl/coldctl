# syntax=docker/dockerfile:1
FROM rust:1.88-bookworm@sha256:af306cfa71d987911a781c37b59d7d67d934f49684058f96cf72079c3626bfe0 AS toolchain
FROM ubuntu:24.04@sha256:24737cef17f4e22b1977df7d9a05e77c4e28b7da5b8911f8d8ccda136bf3573c AS build
COPY --from=toolchain /usr/local/rustup /usr/local/rustup
COPY --from=toolchain /usr/local/cargo /usr/local/cargo
ENV RUSTUP_HOME=/usr/local/rustup CARGO_HOME=/usr/local/cargo PATH=/usr/local/cargo/bin:$PATH
RUN apt-get update && apt-get install -y --no-install-recommends build-essential pkg-config libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates crates
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked --mount=type=cache,target=/src/target,sharing=locked cargo build --release --locked -p coldctl -j 2 && cp target/release/coldctl /coldctl

FROM ubuntu:24.04@sha256:24737cef17f4e22b1977df7d9a05e77c4e28b7da5b8911f8d8ccda136bf3573c
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3t64 && rm -rf /var/lib/apt/lists/* && groupadd -g 10001 coldctl && useradd -u 10001 -g 10001 -M coldctl && mkdir -p /var/lib/coldctl /archives && chown 10001:10001 /var/lib/coldctl /archives
COPY --from=build /coldctl /usr/local/bin/coldctl
COPY --chmod=755 deploy/docker/entrypoint.sh /usr/local/bin/entrypoint
USER 10001:10001
WORKDIR /var/lib/coldctl
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/entrypoint"]
CMD ["agent", "scheduler", "run"]
