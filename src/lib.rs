#[cfg(feature = "server")]
mod error;
pub mod multipart_upload;
pub mod objects;
#[cfg(feature = "server")]
mod open_api;
#[cfg(feature = "server")]
mod s3_configuration;

#[cfg(feature = "server")]
pub use server::*;

#[cfg(feature = "server")]
mod server {
  pub use crate::{error::Error, open_api::*, s3_configuration::S3Configuration};

  use serde::Serialize;
  use warp::{
    hyper::{
      header::{ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_ORIGIN, CONTENT_TYPE, LOCATION},
      Body, Response, StatusCode,
    },
    Filter, Rejection, Reply,
  };

  pub fn routes(
    s3_configuration: &S3Configuration,
  ) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    crate::multipart_upload::routes(s3_configuration).or(crate::objects::routes(s3_configuration))
  }

  pub fn request_builder() -> warp::http::response::Builder {
    warp::hyper::Response::builder()
      .header(ACCESS_CONTROL_ALLOW_HEADERS, "*")
      .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
  }

  pub(crate) fn to_ok_json_response<T>(body_response: &T) -> Result<Response<Body>, Rejection>
  where
    T: Serialize + ?Sized,
  {
    let json = serde_json::to_string(body_response)
      .map_err(|error| warp::reject::custom(Error::JsonError(error)))?;

    request_builder()
      .header(CONTENT_TYPE, "application/json")
      .status(StatusCode::OK)
      .body(json.into())
      .map_err(|error| warp::reject::custom(Error::HttpError(error)))
  }

  pub(crate) fn to_redirect_response(url: &str) -> Result<Response<Body>, Rejection> {
    request_builder()
      .header(LOCATION, url)
      .status(StatusCode::FOUND)
      .body(Body::empty())
      .map_err(|error| warp::reject::custom(Error::HttpError(error)))
  }
}
