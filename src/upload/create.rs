use crate::{
  s3_configuration::S3Configuration, to_ok_json_response, upload::execute_s3_request_operation,
};
use rusoto_s3::{CreateMultipartUploadRequest, S3};
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use utoipa::ToSchema;
use warp::{
  hyper::{Body, Response, StatusCode},
  Filter, Rejection, Reply,
};

#[derive(Debug, Deserialize)]
struct CreateUploadQueryParameters {
  bucket: String,
  path: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct CreateUploadResponse {
  upload_id: String,
}

/// Create multipart upload
#[utoipa::path(
  post,
  context_path = "/api/multipart-upload",
  path = "",
  tag = "Multipart upload",
  responses(
    (status = 200, description = "Successfully created multipart upload", body = CreateUploadResponse),
  ),
  params(
    ("bucket" = String, Query, description = "Name of the bucket"),
    ("path" = String, Query, description = "Key of the object to upload")
  ),
)]
pub(crate) fn route(
  s3_configuration: &S3Configuration,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
  let s3_configuration = s3_configuration.clone();
  warp::path::end()
    .and(warp::post())
    .and(warp::query::<CreateUploadQueryParameters>())
    .and_then(move |parameters: CreateUploadQueryParameters| {
      handle_create_multipart_upload(s3_configuration.clone(), parameters.bucket, parameters.path)
    })
}

async fn handle_create_multipart_upload(
  s3_configuration: S3Configuration,
  bucket: String,
  key: String,
) -> Result<Response<Body>, Infallible> {
  log::info!("Create multipart upload...");
  execute_s3_request_operation(&s3_configuration, |client| async move {
    let request = CreateMultipartUploadRequest {
      bucket,
      key,
      ..Default::default()
    };

    let result = client.create_multipart_upload(request).await;
    if let Ok(output) = &result {
      if let Some(upload_id) = &output.upload_id {
        let body_response = CreateUploadResponse {
          upload_id: upload_id.clone(),
        };
        return Ok(to_ok_json_response(&body_response));
      }
      log::error!("Invalid create_multipart_upload response: {:?}", output);
    }

    log::error!(
      "Failure on create_multipart_upload: {}",
      result.unwrap_err()
    );
    Ok(StatusCode::INTERNAL_SERVER_ERROR.into_response())
  })
  .await
}
