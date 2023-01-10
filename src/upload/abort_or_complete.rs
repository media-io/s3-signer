use crate::{s3_configuration::S3Configuration, upload::execute_s3_request_operation};
use rusoto_s3::{
  AbortMultipartUploadRequest, CompleteMultipartUploadRequest, CompletedMultipartUpload,
  CompletedPart, S3,
};
use serde::Deserialize;
use std::convert::Infallible;
use warp::{
  hyper::{Body, Response, StatusCode},
  Filter, Rejection, Reply,
};

#[derive(Debug, Deserialize)]
struct AbortOrCompleteUploadQueryParameters {
  bucket: String,
  path: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action")]
enum AbortOrCompleteUploadBody {
  Abort,
  Complete { parts: Vec<(i64, String)> },
}

pub(crate) fn route(
  s3_configuration: &S3Configuration,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
  let s3_configuration = s3_configuration.clone();
  warp::path!(String)
    .and(warp::post())
    .and(warp::query::<AbortOrCompleteUploadQueryParameters>())
    .and(warp::body::json::<AbortOrCompleteUploadBody>())
    .map(
      move |upload_id: String,
            parameters: AbortOrCompleteUploadQueryParameters,
            body: AbortOrCompleteUploadBody| {
        (
          upload_id,
          parameters.bucket,
          parameters.path,
          body,
          s3_configuration.clone(),
        )
      },
    )
    .and_then(
      |(upload_id, bucket, path, body, s3_configuration): (
        String,
        String,
        String,
        AbortOrCompleteUploadBody,
        S3Configuration,
      )| async move {
        match body {
          AbortOrCompleteUploadBody::Abort => {
            handle_abort_multipart_upload(s3_configuration.clone(), bucket, path, upload_id).await
          }
          AbortOrCompleteUploadBody::Complete { parts } => {
            handle_complete_multipart_upload(
              s3_configuration.clone(),
              bucket,
              path,
              upload_id,
              parts,
            )
            .await
          }
        }
      },
    )
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

async fn handle_complete_multipart_upload(
  s3_configuration: S3Configuration,
  bucket: String,
  key: String,
  upload_id: String,
  body: Vec<(i64, String)>,
) -> Result<Response<Body>, Infallible> {
  log::info!("Complete multipart upload: upload_id={}", upload_id);
  execute_s3_request_operation(&s3_configuration, |client| async move {
    let parts = body
      .into_iter()
      .map(|(part_number, e_tag)| CompletedPart {
        part_number: Some(part_number),
        e_tag: Some(e_tag),
      })
      .collect();
    let parts = CompletedMultipartUpload { parts: Some(parts) };

    let request = CompleteMultipartUploadRequest {
      bucket,
      key,
      upload_id,
      multipart_upload: Some(parts),
      ..Default::default()
    };

    if let Err(error) = client.complete_multipart_upload(request).await {
      log::error!("Failure on complete_multipart_upload: {}", error);
      Ok(StatusCode::INTERNAL_SERVER_ERROR.into_response())
    } else {
      Ok(StatusCode::OK.into_response())
    }
  })
  .await
}
