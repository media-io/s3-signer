use async_std::{
  net::{TcpListener, TcpStream},
  prelude::*,
  task,
};
use clap::Parser;
use http_types::{Method, Response, StatusCode};
use rusoto_credential::{AwsCredentials, StaticProvider};
use rusoto_s3::{
  util::{PreSignedRequest, PreSignedRequestOption},
  GetObjectRequest, ListObjectsV2Request, PutObjectRequest, S3Client, S3,
};
use rusoto_signature::Region;
use serde::{Deserialize, Serialize};
use simple_logger::SimpleLogger;
use std::str::FromStr;

pub mod built_info {
  include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

#[derive(Clone, Debug)]
struct S3Configuration {
  s3_access_key_id: String,
  s3_secret_access_key: String,
  s3_region: Region,
}

/// S3 Signer for AWS and other S3 compatible storage systems
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
  /// Sets the AWS Access Key ID
  #[clap(
    long,
    value_parser,
    name = "aws-access-key-id",
    env = "AWS_ACCESS_KEY_ID"
  )]
  aws_access_key_id: String,

  /// Sets the AWS Secret Access Key
  #[clap(
    long,
    value_parser,
    name = "aws-secret-access-key",
    env = "AWS_SECRET_ACCESS_KEY"
  )]
  aws_secret_access_key: String,

  /// Sets the AWS Region
  #[clap(
    short,
    long,
    value_parser,
    name = "aws-region",
    env = "AWS_REGION",
    default_value = "us-east-1"
  )]
  aws_region: String,

  /// Sets the AWS Hostname (required for non-AWS S3 endpoint)
  #[clap(short, long, value_parser, env = "AWS_HOSTNAME")]
  hostname: Option<String>,

  /// Sets the port number to server the signer
  #[clap(short, long, value_parser, env = "PORT", default_value_t = 8000)]
  port: u16,

  /// Sets the level of verbosity
  #[clap(short, long, parse(from_occurrences))]
  verbose: usize,
}

#[async_std::main]
async fn main() -> http_types::Result<()> {
  let args = Args::parse();

  let log_level = match args.verbose {
    0 => log::LevelFilter::Error,
    1 => log::LevelFilter::Warn,
    2 => log::LevelFilter::Info,
    3 => log::LevelFilter::Debug,
    _ => log::LevelFilter::Trace,
  };

  SimpleLogger::new().with_level(log_level).init().unwrap();

  let aws_region = if let Some(aws_hostname) = args.hostname {
    Region::Custom {
      name: args.aws_region,
      endpoint: aws_hostname,
    }
  } else {
    Region::from_str(&args.aws_region).unwrap()
  };

  let s3_configuration = S3Configuration {
    s3_access_key_id: args.aws_access_key_id,
    s3_secret_access_key: args.aws_secret_access_key,
    s3_region: aws_region,
  };

  // Open up a TCP connection and create a URL.
  let listener = TcpListener::bind(("0.0.0.0", args.port)).await?;
  let addr = format!("http://{}", listener.local_addr()?);
  log::info!("listening on {}", addr);

  // For each incoming TCP connection, spawn a task and call `accept`.
  let mut incoming = listener.incoming();
  while let Some(stream) = incoming.next().await {
    let stream = stream?;
    let s3_configuration_cloned = s3_configuration.clone();
    task::spawn(async move {
      if let Err(err) = accept(stream, &s3_configuration_cloned).await {
        log::error!("{}", err);
      }
    });
  }
  Ok(())
}

fn default_as_false() -> bool {
  false
}

#[derive(Debug, Deserialize)]
struct QueryParameters {
  bucket: String,
  path: String,
  #[serde(default = "default_as_false")]
  list: bool,
  #[serde(default = "default_as_false")]
  create: bool,
}

#[derive(Debug, Serialize)]
struct PresignedUrlResponse {
  url: String,
}

// Take a TCP stream, and convert it into sequential HTTP request / response pairs.
async fn accept(stream: TcpStream, s3_configuration: &S3Configuration) -> http_types::Result<()> {
  log::info!("starting new connection from {}", stream.peer_addr()?);
  async_h1::accept(stream.clone(), |request| async move {
    log::trace!("{:?}", request);

    if request.url().path() == "/" {
      let mut response = Response::new(StatusCode::Ok);
      response.set_body(format!("S3 Signer (version {})", built_info::PKG_VERSION));
      return Ok(response);
    }

    if request.url().path() == "/api/sign" {
      if request.method() == Method::Options {
        let mut response = Response::new(StatusCode::Ok);
        response.insert_header("Allow", "GET, OPTIONS, HEAD");
        response.insert_header("Access-Control-Allow-Origin", "*");
        response.insert_header(
          "Access-Control-Allow-Methods",
          "GET, POST, PUT, DELETE, PATCH, OPTIONS",
        );
        response.insert_header("Access-Control-Allow-Headers", "*");
        return Ok(response);
      }

      if let Ok(QueryParameters {
        bucket,
        path,
        list,
        create,
      }) = request.query()
      {
        let credentials = AwsCredentials::new(
          &s3_configuration.s3_access_key_id,
          &s3_configuration.s3_secret_access_key,
          None,
          None,
        );

        if list {
          let result = list_directory(s3_configuration, &bucket, Some(path));

          let mut response = Response::new(StatusCode::Ok);
          response.insert_header("Content-Type", "application/json");
          response.set_body(result);
          return Ok(response);
        }

        let presigned_url = if create {
          let put_object = PutObjectRequest {
            bucket,
            key: path,
            ..Default::default()
          };

          put_object.get_presigned_url(
            &s3_configuration.s3_region,
            &credentials,
            &PreSignedRequestOption::default(),
          )
        } else {
          let get_object = GetObjectRequest {
            bucket,
            key: path,
            ..Default::default()
          };

          get_object.get_presigned_url(
            &s3_configuration.s3_region,
            &credentials,
            &PreSignedRequestOption::default(),
          )
        };

        let body_response = PresignedUrlResponse { url: presigned_url };

        let mut response = Response::new(StatusCode::Ok);
        response.insert_header("Access-Control-Allow-Headers", "*");
        response.insert_header("Access-Control-Allow-Origin", "*");
        response.insert_header(
          "Access-Control-Allow-Methods",
          "GET, POST, PUT, DELETE, PATCH, OPTIONS",
        );
        response.insert_header("Content-Type", "application/json");
        response.set_body(serde_json::to_string(&body_response).unwrap().as_bytes());
        Ok(response)
      } else {
        Ok(Response::new(StatusCode::UnprocessableEntity))
      }
    } else {
      Ok(Response::new(StatusCode::NotFound))
    }
  })
  .await?;
  Ok(())
}

#[derive(Debug, Serialize)]
struct Object {
  path: String,
  is_dir: bool,
}

fn list_directory(
  s3_configuration: &S3Configuration,
  bucket: &str,
  source_prefix: Option<String>,
) -> String {
  use tokio::runtime::Runtime;

  let runtime = Runtime::new().unwrap();

  runtime.block_on(async {
    let credentials = AwsCredentials::new(
      &s3_configuration.s3_access_key_id,
      &s3_configuration.s3_secret_access_key,
      None,
      None,
    );

    let list_objects = ListObjectsV2Request {
      bucket: bucket.to_string(),
      delimiter: Some(String::from("/")),
      prefix: source_prefix.clone(),
      ..Default::default()
    };

    let http_client = rusoto_core::request::HttpClient::new().unwrap();
    let credentials: StaticProvider = credentials.into();

    let client = S3Client::new_with(http_client, credentials, s3_configuration.s3_region.clone());

    let response = client.list_objects_v2(list_objects).await.unwrap();

    let mut objects = response
      .contents
      .map(|contents| {
        contents
          .iter()
          .filter_map(|content| build_object(&content.key, &source_prefix, false))
          .collect::<Vec<_>>()
      })
      .unwrap_or_default();

    let mut folders = response
      .common_prefixes
      .map(|prefixes| {
        prefixes
          .iter()
          .filter_map(|prefix| build_object(&prefix.prefix, &source_prefix, true))
          .collect::<Vec<_>>()
      })
      .unwrap_or_default();

    objects.append(&mut folders);

    serde_json::to_string(&objects).unwrap()
  })
}

fn build_object(path: &Option<String>, prefix: &Option<String>, is_dir: bool) -> Option<Object> {
  let prefix_len = prefix.as_ref().map(|s| s.len()).unwrap_or(0);
  let path = path
    .clone()
    .unwrap_or_else(|| "".to_string())
    .split_off(prefix_len);

  if path.is_empty() {
    return None;
  }

  Some(Object { path, is_dir })
}
