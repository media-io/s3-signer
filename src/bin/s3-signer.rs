use clap::Parser;
use s3_signer::S3Configuration;
use simple_logger::SimpleLogger;
use std::convert::Infallible;
use utoipa::OpenApi;
use warp::{
  hyper::{header::ACCESS_CONTROL_ALLOW_METHODS, Body, StatusCode},
  Filter, Rejection, Reply,
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
    long,
    value_parser,
    name = "aws-region",
    env = "AWS_REGION",
    default_value = "us-east-1"
  )]
  aws_region: String,

  /// Sets the AWS Hostname (required for non-AWS S3 endpoint)
  #[clap(short, long, value_parser, env = "AWS_HOSTNAME")]
  aws_hostname: Option<String>,

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

  let s3_configuration = if let Some(aws_hostname) = args.aws_hostname {
    S3Configuration::new_with_hostname(
      &args.aws_access_key_id,
      &args.aws_secret_access_key,
      &args.aws_region,
      &aws_hostname,
    )
  } else {
    S3Configuration::new(
      &args.aws_access_key_id,
      &args.aws_secret_access_key,
      &args.aws_region,
    )
    .unwrap()
  };

  start(&s3_configuration, args.port).await;

  Ok(())
}

const API_ROOT_PATH: &str = "api";

async fn start(s3_configuration: &S3Configuration, port: u16) {
  let routes = root()
    .or(options())
    .or(warp::path(API_ROOT_PATH).and(s3_signer::routes(s3_configuration)))
    .or(doc())
    .recover(handle_rejection);

  warp::serve(routes).run(([0, 0, 0, 0], port)).await;
}

#[derive(OpenApi)]
#[openapi(
  paths(root),
  tags(
    (name = "Server", description = "S3 Signer server API"),
  )
)]
struct ApiDoc;

/// Root path
#[utoipa::path(
  get,
  path = "/",
  tag = "Server",
  responses((status = 200, description = "Server information"))
)]
fn root() -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
  warp::path::end().and(warp::get()).map(|| {
    format!(
      "S3 Signer (version {})\nAPI documentation on: /swagger-ui/",
      built_info::PKG_VERSION
    )
  })
}

fn options() -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
  warp::options().map(|| {
    s3_signer::request_builder()
      .header(ACCESS_CONTROL_ALLOW_METHODS, "GET, OPTIONS, POST, PUT")
      .body(Body::empty())
      .unwrap()
  })
}

fn doc() -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
  let open_api_doc = s3_signer::insert_open_api_at(ApiDoc::openapi(), API_ROOT_PATH);

  let api_doc = warp::path("api-doc.json")
    .and(warp::get())
    .map(move || warp::reply::json(&open_api_doc));

  let swagger = s3_signer::swagger_route("swagger-ui", "api-doc.json");

  api_doc.or(swagger)
}

async fn handle_rejection(err: Rejection) -> Result<impl Reply, Infallible> {
  if err.is_not_found() {
    return Ok(StatusCode::NOT_FOUND.into_response());
  }

  if let Some(error) = err.find::<s3_signer::Error>() {
    log::error!("{}", error);
  } else {
    log::error!("Unhandled rejection: {:?}", err);
  }
  Ok(StatusCode::INTERNAL_SERVER_ERROR.into_response())
}
