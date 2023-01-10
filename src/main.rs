mod s3_configuration;
mod sign;
mod upload;

use crate::s3_configuration::S3Configuration;
use clap::Parser;
use rusoto_signature::Region;
use serde::Serialize;
use simple_logger::SimpleLogger;
use std::str::FromStr;
use warp::{
  hyper::header::{
    ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN,
    CONTENT_TYPE,
  },
  Filter,
};

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

#[tokio::main]
async fn main() -> std::io::Result<()> {
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

  start(&s3_configuration, args.port).await;

  Ok(())
}

async fn start(s3_configuration: &S3Configuration, port: u16) {
  let root = warp::path::end()
    .and(warp::get())
    .map(|| format!("S3 Signer (version {})", built_info::PKG_VERSION));

  let api =
    warp::path("api").and(upload::routes(s3_configuration).or(sign::routes(s3_configuration)));

  let routes = root.or(api);

  warp::serve(routes).run(([127, 0, 0, 1], port)).await;
}

pub(crate) fn request_builder() -> warp::http::response::Builder {
  warp::hyper::Response::builder()
    .header(ACCESS_CONTROL_ALLOW_HEADERS, "*")
    .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
    .header(
      ACCESS_CONTROL_ALLOW_METHODS,
      "GET, POST, PUT, DELETE, PATCH, OPTIONS",
    )
}

pub(crate) fn to_ok_json_response<T>(body_response: &T) -> warp::hyper::Response<warp::hyper::Body>
where
  T: Serialize + ?Sized,
{
  request_builder()
    .header(CONTENT_TYPE, "application/json")
    .body(serde_json::to_string(body_response).unwrap().into())
    .unwrap() // TODO handle
}
