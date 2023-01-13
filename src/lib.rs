mod error;
mod objects;
mod open_api;
mod s3_configuration;
mod upload;

pub use crate::{error::Error, open_api::*, s3_configuration::S3Configuration};

use serde::Serialize;
use warp::{
  hyper::{
    header::{
      ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN,
      CONTENT_TYPE, LOCATION,
    },
    Body, Response, StatusCode,
  },
  Filter, Rejection, Reply,
};

pub fn routes(
  s3_configuration: &S3Configuration,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
  upload::routes(s3_configuration).or(objects::routes(s3_configuration))
}

pub fn request_builder() -> warp::http::response::Builder {
  warp::hyper::Response::builder()
    .header(ACCESS_CONTROL_ALLOW_HEADERS, "*")
    .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
    .header(
      ACCESS_CONTROL_ALLOW_METHODS,
      "GET, POST, PUT, DELETE, PATCH, OPTIONS",
    )
}

pub(crate) fn to_ok_json_response<T>(body_response: &T) -> Result<Response<Body>, Rejection>
where
  T: Serialize + ?Sized,
{
  request_builder()
    .header(CONTENT_TYPE, "application/json")
    .status(StatusCode::OK)
    .body(serde_json::to_string(body_response).unwrap().into())
    .map_err(|error| warp::reject::custom(Error::HttpError(error)))
}

pub(crate) fn to_redirect_response(url: &str) -> Result<Response<Body>, Rejection> {
  request_builder()
    .header(LOCATION, url)
    .status(StatusCode::FOUND)
    .body(Body::empty())
    .map_err(|error| warp::reject::custom(Error::HttpError(error)))
}
