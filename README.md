# s3-signer
A simple S3 signer in Rust.

## Supported storages
It's actually tested with [MinIO](https://min.io/).
It will also support AWS S3.

## Installation
```
cargo install s3-signer
```

## Run service
```
export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
s3-signer --aws-hostname http://localhost:9000
```
