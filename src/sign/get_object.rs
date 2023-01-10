use crate::{s3_configuration::S3Configuration, sign::PresignedUrlResponse, to_ok_json_response};
use rusoto_credential::AwsCredentials;
use rusoto_s3::{
  util::{PreSignedRequest, PreSignedRequestOption},
  GetObjectRequest,
};
use std::convert::Infallible;
use warp::hyper::{Body, Response};

pub(crate) async fn handle_get_object_signed_url(
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

  let body_response = PresignedUrlResponse { url: presigned_url };

  Ok(to_ok_json_response(&body_response))
}
