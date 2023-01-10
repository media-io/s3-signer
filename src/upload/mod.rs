mod abort;
mod complete;
mod create;
mod part_upload_url;

use crate::S3Configuration;
use rusoto_s3::S3Client;
use std::convert::{Infallible, TryFrom};
use warp::{hyper, Filter, Rejection, Reply};

pub(crate) fn routes(
  s3_configuration: &S3Configuration,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
  warp::path("upload").and(
    create::route(s3_configuration)
      .or(part_upload_url::route(s3_configuration))
      .or(complete::route(s3_configuration))
      .or(abort::route(s3_configuration)),
  )
}

async fn execute_s3_request_operation<F, Fut>(
  s3_configuration: &S3Configuration,
  operation: F,
) -> Result<hyper::Response<hyper::Body>, Infallible>
where
  F: FnOnce(S3Client) -> Fut,
  Fut: std::future::Future<Output = Result<hyper::Response<hyper::Body>, Infallible>>,
{
  let result = S3Client::try_from(s3_configuration);
  if let Err(error) = result {
    log::error!("Cannot create S3 client: {}", error);
    return Ok(hyper::StatusCode::INTERNAL_SERVER_ERROR.into_response());
  }

  let client = result.unwrap();

  operation(client).await
}
