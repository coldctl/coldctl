FROM rust:1.88-bookworm AS rust
FROM postgres:16-bookworm
COPY --from=rust /usr/local/rustup /usr/local/rustup
COPY --from=rust /usr/local/cargo /usr/local/cargo
ENV RUSTUP_HOME=/usr/local/rustup CARGO_HOME=/usr/local/cargo
ENV PATH=/usr/local/cargo/bin:$PATH
RUN apt-get update && apt-get install -y --no-install-recommends build-essential pkg-config libssl-dev cmake python3 postgresql-16-repack && rm -rf /var/lib/apt/lists/*
