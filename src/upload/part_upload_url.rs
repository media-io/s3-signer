use crate::{s3_configuration::S3Configuration, sign::PresignedUrlResponse, to_ok_json_response};
use rusoto_credential::AwsCredentials;
use rusoto_s3::{
  util::{PreSignedRequest, PreSignedRequestOption},
  UploadPartRequest,
};
use serde::Deserialize;
use std::convert::Infallible;
use warp::{
  hyper::{Body, Response},
  Filter, Rejection, Reply,
};

#[derive(Debug, Deserialize)]
struct PartUploadQueryParameters {
  bucket: String,
  path: String,
  upload_id: String,
  part_number: i64,
}

pub(crate) fn route(
  s3_configuration: &S3Configuration,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
  let s3_configuration = s3_configuration.clone();
  warp::get()
    .and(warp::query::<PartUploadQueryParameters>())
    .and_then(move |parameters: PartUploadQueryParameters| {
      handle_part_upload_presigned_url(
        s3_configuration.clone(),
        parameters.bucket,
        parameters.path,
        parameters.upload_id,
        parameters.part_number,
      )
    })
}

async fn handle_part_upload_presigned_url(
  s3_configuration: S3Configuration,
  bucket: String,
  key: String,
  upload_id: String,
  part_number: i64,
) -> Result<Response<Body>, Infallible> {
  log::info!(
    "Upload part: upload_id={}, part_number={}",
    upload_id,
    part_number,
  );
  let request = UploadPartRequest {
    bucket,
    key,
    upload_id,
    part_number,
    ..Default::default()
  };

  let credentials = AwsCredentials::from(&s3_configuration);

  let presigned_url = request.get_presigned_url(
    &s3_configuration.s3_region,
    &credentials,
    &PreSignedRequestOption::default(),
  );

  let body_response = PresignedUrlResponse { url: presigned_url };
  Ok(to_ok_json_response(&body_response))
}
