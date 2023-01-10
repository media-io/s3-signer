mod create_object;
mod get_object;
mod list_objects;

use crate::{request_builder, S3Configuration};
use serde::{Deserialize, Serialize};
use warp::{hyper::header::ALLOW, Filter, Rejection, Reply};

#[derive(Debug, Clone, Deserialize)]
struct SignQueryParameters {
  bucket: String,
  path: String,
  #[serde(default = "default_as_false")]
  list: bool,
  #[serde(default = "default_as_false")]
  create: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct PresignedUrlResponse {
  pub(crate) url: String,
}

fn default_as_false() -> bool {
  false
}

pub(crate) fn routes(
  s3_configuration: &S3Configuration,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
  warp::path("sign").and(options().or(sign(s3_configuration)))
}

pub(crate) fn sign(
  s3_configuration: &S3Configuration,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
  let s3_configuration = s3_configuration.clone();
  warp::get()
    .and(warp::query::<SignQueryParameters>())
    .map(move |parameters: SignQueryParameters| (parameters, s3_configuration.clone()))
    .and_then(
      |(parameters, s3_configuration): (SignQueryParameters, S3Configuration)| async move {
        match parameters {
          SignQueryParameters {
            bucket,
            path,
            list: true,
            create: _,
          } => {
            list_objects::handle_list_objects_signed_url(s3_configuration, bucket, Some(path)).await
          }
          SignQueryParameters {
            bucket,
            path,
            list: false,
            create: true,
          } => create_object::handle_create_object_signed_url(s3_configuration, bucket, path).await,
          SignQueryParameters {
            bucket,
            path,
            list: false,
            create: false,
          } => get_object::handle_get_object_signed_url(s3_configuration, bucket, path).await,
        }
      },
    )
}

fn options() -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
  warp::options().map(|| {
    request_builder()
      .header(ALLOW, "GET, OPTIONS, HEAD")
      .body(warp::hyper::Body::empty())
      .unwrap()
  })
}
