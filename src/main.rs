mod s3_configuration;
mod sign;
mod upload;

use crate::s3_configuration::S3Configuration;
use async_std::{
  net::{TcpListener, TcpStream},
  prelude::*,
  task,
};
use clap::Parser;
use http_types::{Response, StatusCode};
use rusoto_signature::Region;
use serde::Serialize;
use simple_logger::SimpleLogger;
use std::str::FromStr;

pub mod built_info {
  include!(concat!(env!("OUT_DIR"), "/built.rs"));
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

// Take a TCP stream, and convert it into sequential HTTP request / response pairs.
async fn accept(stream: TcpStream, s3_configuration: &S3Configuration) -> http_types::Result<()> {
  log::info!("starting new connection from {}", stream.peer_addr()?);
  async_h1::accept(stream.clone(), |mut request| async move {
    log::trace!("{:?}", request);

    if request.url().path() == "/" {
      let mut response = Response::new(StatusCode::Ok);
      response.set_body(format!("S3 Signer (version {})", built_info::PKG_VERSION));
      return Ok(response);
    }

    match request.url().path() {
      "/api/upload" => upload::handle_upload_request(&mut request, s3_configuration),
      "/api/sign" => sign::handle_signature_request(&request, s3_configuration),
      _ => Ok(Response::new(StatusCode::NotFound)),
    }
  })
  .await?;
  Ok(())
}

pub(crate) fn to_ok_json_response<T>(body_response: &T) -> Response
where
  T: Serialize + ?Sized,
{
  let mut response = Response::new(StatusCode::Ok);
  response.insert_header("Access-Control-Allow-Headers", "*");
  response.insert_header("Access-Control-Allow-Origin", "*");
  response.insert_header(
    "Access-Control-Allow-Methods",
    "GET, POST, PUT, DELETE, PATCH, OPTIONS",
  );
  response.insert_header("Content-Type", "application/json");
  response.set_body(serde_json::to_string(body_response).unwrap().as_bytes());

  response
}
