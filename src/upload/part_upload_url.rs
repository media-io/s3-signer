use crate::{s3_configuration::S3Configuration, to_redirect_response};
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
}

/// Pre-sign part upload URL
#[utoipa::path(
  put,
  context_path = "/api/multipart-upload",
  path = "/{upload_id}/part/{part_number}",
  tag = "Multipart upload",
  responses(
    (status = 302, description = "Redirect to pre-signed URL for getting an object"),
  ),
  params(
    ("upload_id" = String, Path, description = "ID of the upload"),
    ("part_number" = i64, Path, description = "Number of the part to upload"),
    ("bucket" = String, Query, description = "Name of the bucket"),
    ("path" = String, Query, description = "Key of the object to get")
  ),
)]
pub(crate) fn route(
  s3_configuration: &S3Configuration,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
  let s3_configuration = s3_configuration.clone();
  warp::path!(String / "part" / i64)
    .and(warp::put())
    .and(warp::query::<PartUploadQueryParameters>())
    .and_then(
      move |upload_id: String, part_number: i64, parameters: PartUploadQueryParameters| {
        handle_part_upload_presigned_url(
          s3_configuration.clone(),
          parameters.bucket,
          parameters.path,
          upload_id,
          part_number,
        )
      },
    )
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

  Ok(to_redirect_response(&presigned_url))
}
