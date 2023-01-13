use crate::{objects::SignQueryParameters, to_redirect_response, S3Configuration};
use rusoto_credential::AwsCredentials;
use rusoto_s3::{
  util::{PreSignedRequest, PreSignedRequestOption},
  PutObjectRequest,
};
use std::convert::Infallible;
use warp::{
  hyper::{Body, Response},
  Filter, Rejection, Reply,
};

/// Pre-sign object creation URL
#[utoipa::path(
  post,
  path = "/objects",
  tag = "Objects",
  responses(
    (status = 302, description = "Redirect to pre-signed URL for object creation"),
  ),
  params(
    ("bucket" = String, Query, description = "Name of the bucket"),
    ("path" = String, Query, description = "Key of the object to create")
  ),
)]
pub(crate) fn route(
  s3_configuration: &S3Configuration,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
  let s3_configuration = s3_configuration.clone();
  warp::path("objects")
    .and(warp::post())
    .and(warp::query::<SignQueryParameters>())
    .and(warp::any().map(move || s3_configuration.clone()))
    .and_then(
      |parameters: SignQueryParameters, s3_configuration: S3Configuration| async move {
        handle_create_object_signed_url(s3_configuration, parameters.bucket, parameters.path).await
      },
    )
}

async fn handle_create_object_signed_url(
  s3_configuration: S3Configuration,
  bucket: String,
  key: String,
) -> Result<Response<Body>, Infallible> {
  log::info!("Create object signed URL: bucket={}, key={}", bucket, key);
  let credentials = AwsCredentials::from(&s3_configuration);

  let put_object = PutObjectRequest {
    bucket,
    key,
    ..Default::default()
  };

  let presigned_url = put_object.get_presigned_url(
    s3_configuration.region(),
    &credentials,
    &PreSignedRequestOption::default(),
  );

  Ok(to_redirect_response(&presigned_url))
}
