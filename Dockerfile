# build sqld
FROM rust:alpine AS chef
RUN apk add --no-cache \
        clang-dev clang llvm-dev \
        build-base tcl protobuf file \
        openssl-dev pkgconfig git cmake \
        musl-dev

# We need to install and set as default the toolchain specified in rust-toolchain.toml
# Otherwise cargo-chef will build dependencies using wrong toolchain
# This also prevents planner and builder steps from installing the toolchain over and over again
COPY rust-toolchain.toml rust-toolchain.toml
RUN cat rust-toolchain.toml | grep "channel" | awk '{print $3}' | sed 's/"//g' > toolchain.txt \
    && rustup update $(cat toolchain.txt) \
    && rustup default $(cat toolchain.txt) \
    && rm toolchain.txt rust-toolchain.toml \
    && cargo install cargo-chef --version 0.1.75 --locked

FROM chef AS planner
ARG BUILD_DEBUG=false
ENV CARGO_PROFILE_RELEASE_DEBUG=$BUILD_DEBUG
RUN echo $CARGO_PROFILE_RELEASE_DEBUG
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ARG BUILD_DEBUG=false
ENV CARGO_PROFILE_RELEASE_DEBUG=$BUILD_DEBUG
ENV RUSTFLAGS='-C target-feature=-crt-static --cfg tokio_unstable'
COPY --from=planner /recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json -p libsql-server -p bottomless-cli
COPY . .
ARG ENABLE_FEATURES=""
RUN if [ "$ENABLE_FEATURES" == "" ]; then \
        cargo build -p libsql-server --release ; \
    else \
        cargo build -p libsql-server --features "$ENABLE_FEATURES" --release ; \
    fi
RUN cargo build -p bottomless-cli --release

# runtime
FROM alpine
RUN apk add --no-cache bash su-exec ca-certificates libgcc

EXPOSE 5001 8080
VOLUME [ "/var/lib/sqld" ]

RUN addgroup -S -g 666 sqld
RUN adduser -S -u 666 -G sqld -h /var/lib/sqld sqld
WORKDIR /var/lib/sqld
USER sqld

COPY docker-entrypoint.sh /usr/local/bin
COPY docker-wrapper.sh /usr/local/bin

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /target/release/sqld /bin/sqld
COPY --from=builder /target/release/bottomless-cli /bin/bottomless-cli

USER root

ENTRYPOINT ["/usr/local/bin/docker-wrapper.sh"]
CMD ["/bin/sqld"]
