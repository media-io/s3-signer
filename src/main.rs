#[macro_use]
extern crate clap;
#[macro_use]
extern crate serde_derive;

use async_std::{
  net::{TcpListener, TcpStream},
  prelude::*,
  task,
};
use clap::{App, Arg};
use http_types::{Method, Response, StatusCode};
use rusoto_credential::{AwsCredentials, StaticProvider};
use rusoto_s3::{
  util::{PreSignedRequest, PreSignedRequestOption},
  GetObjectRequest, ListObjectsV2Request, S3Client, S3,
};
use rusoto_signature::Region;
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

#[async_std::main]
async fn main() -> http_types::Result<()> {
  let matches = App::new("S3 signer")
    .version(built_info::PKG_VERSION)
    .author("Marc-Antoine Arnaud <maarnaud@media-io.com>")
    .about("S3 Signer for AWS and other S3 compatible storage systems.")
    .arg(
      Arg::with_name("aws-access-key-id")
        .long("aws-access-key-id")
        .value_name("AWS_ACCESS_KEY_ID")
        .env("AWS_ACCESS_KEY_ID")
        .required(true)
        .help("Sets the AWS Access Key ID"),
    )
    .arg(
      Arg::with_name("aws-secret-access-key")
        .long("aws-secret-access-key")
        .value_name("AWS_SECRET_ACCESS_KEY")
        .env("AWS_SECRET_ACCESS_KEY")
        .required(true)
        .help("Sets the AWS Secret Access Key"),
    )
    .arg(
      Arg::with_name("aws-region")
        .long("aws-region")
        .value_name("AWS_REGION")
        .env("AWS_REGION")
        .default_value("us-east-1")
        .help("Sets the AWS Region"),
    )
    .arg(
      Arg::with_name("aws-hostname")
        .long("aws-hostname")
        .value_name("AWS_HOSTNAME")
        .env("AWS_HOSTNAME")
        .help("Sets the AWS Hostname (required for non-AWS S3 endpoint)"),
    )
    .arg(
      Arg::with_name("port")
        .long("port")
        .value_name("PORT")
        .env("PORT")
        .default_value("8000")
        .help("Sets the port number to server the signer (default: 8000)"),
    )
    .arg(
      Arg::with_name("verbose")
        .short("v")
        .multiple(true)
        .help("Sets the level of verbosity"),
    )
    .get_matches();

  let log_level = match matches.occurrences_of("verbose") {
    0 => log::LevelFilter::Error,
    1 => log::LevelFilter::Warn,
    2 => log::LevelFilter::Info,
    3 => log::LevelFilter::Debug,
    _ => log::LevelFilter::Trace,
  };

  SimpleLogger::new().with_level(log_level).init().unwrap();

  let port = value_t!(matches, "port", u16).unwrap_or(8000);

  let aws_hostname = matches.value_of("aws-hostname").map(|s| s.to_string());
  let aws_region = matches
    .value_of("aws-region")
    .map(|s| s.to_string())
    .unwrap();
  let aws_region = if let Some(aws_hostname) = aws_hostname {
    Region::Custom {
      name: aws_region,
      endpoint: aws_hostname,
    }
  } else {
    Region::from_str(&aws_region).unwrap()
  };

  let s3_configuration = S3Configuration {
    s3_access_key_id: matches
      .value_of("aws-access-key-id")
      .map(|s| s.to_string())
      .unwrap(),
    s3_secret_access_key: matches
      .value_of("aws-secret-access-key")
      .map(|s| s.to_string())
      .unwrap(),
    s3_region: aws_region,
  };

  // Open up a TCP connection and create a URL.
  let listener = TcpListener::bind(("0.0.0.0", port)).await?;
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

#[derive(Deserialize)]
struct QueryParameters {
  bucket: String,
  path: String,
  list: bool,
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

      if let Ok(QueryParameters { bucket, path, list }) = request.query() {
        let credentials = AwsCredentials::new(
          &s3_configuration.s3_access_key_id,
          &s3_configuration.s3_secret_access_key,
          None,
          None,
        );

        if list {
          let result = list_directory(&s3_configuration, &bucket, Some(path));

          let mut response = Response::new(StatusCode::Ok);
          response.insert_header("Content-Type", "application/json");
          response.set_body(result);
          return Ok(response);
        }

        let get_object = GetObjectRequest {
          bucket,
          key: path,
          ..Default::default()
        };
        let presigned_url = get_object.get_presigned_url(
          &s3_configuration.s3_region,
          &credentials,
          &PreSignedRequestOption::default(),
        );

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
          .map(|content| build_object(&content.key, &source_prefix, false))
          .filter_map(|content| content)
          .collect::<Vec<_>>()
      })
      .unwrap_or_default();

    let mut folders = response
      .common_prefixes
      .map(|prefixes| {
        prefixes
          .iter()
          .map(|prefix| build_object(&prefix.prefix, &source_prefix, true))
          .filter_map(|content| content)
          .collect::<Vec<_>>()
      })
      .unwrap_or_default();

    objects.append(&mut folders);

    serde_json::to_string(&objects).unwrap()
  })
}

fn build_object(path: &Option<String>, prefix: &Option<String>, is_dir: bool) -> Option<Object> {
  let prefix_len = prefix.as_ref().map(|s| s.len()).unwrap_or(0);
  let path = path.clone().unwrap_or_else(|| "".to_string()).split_off(prefix_len);

  if path.is_empty() {
    return None;
  }

  Some(Object { path, is_dir })
}
