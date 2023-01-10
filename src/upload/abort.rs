use crate::{s3_configuration::S3Configuration, upload::execute_s3_request_operation};
use rusoto_s3::{AbortMultipartUploadRequest, S3};
use serde::Deserialize;
use std::convert::Infallible;
use warp::{
  hyper::{Body, Response, StatusCode},
  Filter, Rejection, Reply,
};

#[derive(Debug, Deserialize)]
struct AbortUploadQueryParameters {
  bucket: String,
  path: String,
  upload_id: String,
}

pub(crate) fn route(
  s3_configuration: &S3Configuration,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
  let s3_configuration = s3_configuration.clone();
  warp::delete()
    .and(warp::query::<AbortUploadQueryParameters>())
    .and_then(move |parameters: AbortUploadQueryParameters| {
      handle_abort_multipart_upload(
        s3_configuration.clone(),
        parameters.bucket,
        parameters.path,
        parameters.upload_id,
      )
    })
}

async fn handle_abort_multipart_upload(
  s3_configuration: S3Configuration,
  bucket: String,
  key: String,
  upload_id: String,
) -> Result<Response<Body>, Infallible> {
  log::info!("Abort multipart upload: upload_id={}", upload_id);
  execute_s3_request_operation(&s3_configuration, |client| async move {
    let request = AbortMultipartUploadRequest {
      bucket,
      key,
      upload_id,
      ..Default::default()
    };

    if let Err(error) = client.abort_multipart_upload(request).await {
      log::error!("Failure on abort_multipart_upload: {}", error);
      Ok(StatusCode::INTERNAL_SERVER_ERROR.into_response())
    } else {
      Ok(StatusCode::OK.into_response())
    }
  })
  .await
}
