use crate::{
  objects::SignQueryParameters, s3_configuration::S3Configuration, to_redirect_response,
};
use rusoto_credential::AwsCredentials;
use rusoto_s3::{
  util::{PreSignedRequest, PreSignedRequestOption},
  GetObjectRequest,
};
use std::convert::Infallible;
use warp::{
  hyper::{Body, Response},
  Filter, Rejection, Reply,
};

pub(crate) fn route(
  s3_configuration: &S3Configuration,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
  let s3_configuration = s3_configuration.clone();
  warp::path("object")
    .and(warp::get())
    .and(warp::query::<SignQueryParameters>())
    .map(move |parameters: SignQueryParameters| (parameters, s3_configuration.clone()))
    .and_then(
      |(parameters, s3_configuration): (SignQueryParameters, S3Configuration)| async move {
        handle_get_object_signed_url(s3_configuration, parameters.bucket, parameters.path).await
      },
    )
}

async fn handle_get_object_signed_url(
  s3_configuration: S3Configuration,
  bucket: String,
  key: String,
) -> Result<Response<Body>, Infallible> {
  let credentials = AwsCredentials::from(&s3_configuration);

  let get_object = GetObjectRequest {
    bucket,
    key,
    ..Default::default()
  };

  let presigned_url = get_object.get_presigned_url(
    &s3_configuration.s3_region,
    &credentials,
    &PreSignedRequestOption::default(),
  );

  Ok(to_redirect_response(&presigned_url))
}
