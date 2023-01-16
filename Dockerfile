FROM rust:1.66-buster as builder

ADD . /src
WORKDIR /src

RUN apt-get update && \
    apt-get install -y libssl-dev && \
    cargo build --verbose --release && \
    cargo install --features server --path .

FROM debian:buster
COPY --from=builder /usr/local/cargo/bin/s3-signer /usr/bin

RUN apt update && \
    apt install -y libssl1.1 ca-certificates

HEALTHCHECK --interval=30s --start-period=2s --retries=2 --timeout=3s CMD curl -v --silent --fail http://localhost:$PORT/ || exit 1

CMD s3-signer
