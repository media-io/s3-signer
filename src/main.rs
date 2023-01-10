mod objects;
mod s3_configuration;
mod upload;

use crate::s3_configuration::S3Configuration;
use clap::Parser;
use rusoto_signature::Region;
use serde::Serialize;
use simple_logger::SimpleLogger;
use std::{str::FromStr, sync::Arc};
use utoipa::OpenApi;
use utoipa_swagger_ui::Config;
use warp::{
  hyper::{
    header::{
      ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN,
      ALLOW, CONTENT_TYPE, LOCATION,
    },
    Body, Response, StatusCode, Uri,
  },
  path::{FullPath, Tail},
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

#[derive(OpenApi)]
#[openapi(
  paths(
    objects::list::route,
    objects::get::route,
    objects::create::route,
    upload::create::route,
    upload::part_upload_url::route,
    upload::abort_or_complete::route,
  ),
  components(
    schemas(
      objects::list::Object,
      upload::create::CreateUploadResponse,
      upload::abort_or_complete::CompletedUploadPart,
      upload::abort_or_complete::AbortOrCompleteUploadBody,
     )
  ),
  tags(
    (name = "Objects", description = "Objects-related API"),
    (name = "Multipart upload", description = "Multipart upload API")
  )
)]
struct ApiDoc;

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
    warp::path("api").and(upload::routes(s3_configuration).or(objects::routes(s3_configuration)));

  let api_doc = warp::path("api-doc.json")
    .and(warp::get())
    .map(|| warp::reply::json(&ApiDoc::openapi()));

  let config = Arc::new(Config::from("/api-doc.json"));

  let swagger_ui = warp::path("swagger-ui")
    .and(warp::get())
    .and(warp::path::full())
    .and(warp::path::tail())
    .and(warp::any().map(move || config.clone()))
    .and_then(serve_swagger);

  let routes = root.or(options()).or(api_doc).or(swagger_ui).or(api);

  warp::serve(routes).run(([127, 0, 0, 1], port)).await;
}

fn options() -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
  warp::options().map(|| {
    request_builder()
      .header(ALLOW, "GET, OPTIONS, POST, PUT")
      .body(Body::empty())
      .unwrap()
  })
}

async fn serve_swagger(
  full_path: FullPath,
  tail: Tail,
  config: Arc<Config<'static>>,
) -> Result<Box<dyn Reply + 'static>, Rejection> {
  if full_path.as_str() == "/swagger-ui" {
    return Ok(Box::new(warp::redirect::found(Uri::from_static(
      "/swagger-ui/",
    ))));
  }

  let path = tail.as_str();
  match utoipa_swagger_ui::serve(path, config) {
    Ok(file) => {
      if let Some(file) = file {
        Ok(Box::new(
          Response::builder()
            .header(CONTENT_TYPE, file.content_type)
            .body(file.bytes),
        ))
      } else {
        Ok(Box::new(StatusCode::NOT_FOUND))
      }
    }
    Err(error) => Ok(Box::new(
      Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(error.to_string()),
    )),
  }
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

pub(crate) fn to_ok_json_response<T>(body_response: &T) -> Response<Body>
where
  T: Serialize + ?Sized,
{
  request_builder()
    .header(CONTENT_TYPE, "application/json")
    .status(StatusCode::OK)
    .body(serde_json::to_string(body_response).unwrap().into())
    .unwrap() // TODO handle
}

pub(crate) fn to_redirect_response(url: &str) -> Response<Body> {
  request_builder()
    .header(LOCATION, url)
    .status(StatusCode::FOUND)
    .body(Body::empty())
    .unwrap() // TODO handle
}
