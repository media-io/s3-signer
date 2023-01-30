use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct AbortOrCompleteUploadQueryParameters {
  pub bucket: String,
  pub path: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[cfg_attr(feature = "server", derive(utoipa::ToSchema))]
#[serde(tag = "action")]
pub enum AbortOrCompleteUploadBody {
  Abort,
  Complete { parts: Vec<CompletedUploadPart> },
}

#[derive(Debug, Deserialize, Serialize)]
#[cfg_attr(feature = "server", derive(utoipa::ToSchema))]
pub struct CompletedUploadPart {
  pub number: i64,
  pub etag: String,
}

#[cfg(feature = "server")]
impl From<CompletedUploadPart> for rusoto_s3::CompletedPart {
  fn from(part: CompletedUploadPart) -> Self {
    Self {
      part_number: Some(part.number),
      e_tag: Some(part.etag),
    }
  }
}

#[cfg(feature = "server")]
pub(crate) mod server {
  use super::{
    AbortOrCompleteUploadBody, AbortOrCompleteUploadQueryParameters, CompletedUploadPart,
  };
  use crate::{multipart_upload::S3Client, to_ok_json_response, Error, S3Configuration};
  use rusoto_s3::{
    AbortMultipartUploadRequest, CompleteMultipartUploadRequest, CompletedMultipartUpload,
    CompletedPart, S3,
  };
  use std::convert::TryFrom;
  use warp::{
    hyper::{Body, Response},
    Filter, Rejection, Reply,
  };

  /// Abort or complete multipart upload
  #[utoipa::path(
    post,
    context_path = "/multipart-upload",
    path = "/{upload_id}",
    tag = "Multipart upload",
    request_body(
      content = AbortOrCompleteUploadBody,
      description = "Description of the abortion or completion request",
      content_type = "application/json"
    ),
    responses(
      (status = 200, description = "Successfully aborted or completed multipart upload"),
    ),
    params(
      ("upload_id" = String, Path, description = "ID of the upload to abort or complete"),
      ("bucket" = String, Query, description = "Name of the bucket"),
      ("path" = String, Query, description = "Key of the object to upload")
    ),
  )]
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
              handle_abort_multipart_upload(&s3_configuration, bucket, path, upload_id).await
            }
            AbortOrCompleteUploadBody::Complete { parts } => {
              handle_complete_multipart_upload(&s3_configuration, bucket, path, upload_id, parts)
                .await
            }
          }
        },
      )
  }

  async fn handle_abort_multipart_upload(
    s3_configuration: &S3Configuration,
    bucket: String,
    key: String,
    upload_id: String,
  ) -> Result<Response<Body>, Rejection> {
    log::info!("Abort multipart upload: upload_id={}", upload_id);
    let client = S3Client::try_from(s3_configuration)?;
    client
      .execute(|client: rusoto_s3::S3Client| async move {
        let request = AbortMultipartUploadRequest {
          bucket,
          key,
          upload_id,
          ..Default::default()
        };

        client
          .abort_multipart_upload(request)
          .await
          .map_err(|error| warp::reject::custom(Error::MultipartUploadAbortionError(error)))
          .and_then(|_output| to_ok_json_response(&()))
      })
      .await
  }

  async fn handle_complete_multipart_upload(
    s3_configuration: &S3Configuration,
    bucket: String,
    key: String,
    upload_id: String,
    body: Vec<CompletedUploadPart>,
  ) -> Result<Response<Body>, Rejection> {
    log::info!("Complete multipart upload: upload_id={}", upload_id);
    let client = S3Client::try_from(s3_configuration)?;
    client
      .execute(|client: rusoto_s3::S3Client| async move {
        let parts = body.into_iter().map(CompletedPart::from).collect();
        let parts = CompletedMultipartUpload { parts: Some(parts) };

        let request = CompleteMultipartUploadRequest {
          bucket,
          key,
          upload_id,
          multipart_upload: Some(parts),
          ..Default::default()
        };

        client
          .complete_multipart_upload(request)
          .await
          .map_err(|error| warp::reject::custom(Error::MultipartUploadCompletionError(error)))
          .and_then(|_output| to_ok_json_response(&()))
      })
      .await
  }
}
