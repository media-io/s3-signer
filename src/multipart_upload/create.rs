use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Deserialize, Serialize)]
pub struct CreateUploadQueryParameters {
  pub bucket: String,
  pub path: String,
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct CreateUploadResponse {
  pub upload_id: String,
}

#[cfg(feature = "server")]
pub(crate) mod server {
  use super::{CreateUploadQueryParameters, CreateUploadResponse};
  use crate::{to_ok_json_response, multipart_upload::S3Client, Error, S3Configuration};
  use rusoto_s3::{CreateMultipartUploadRequest, S3};
  use std::convert::TryFrom;
  use warp::{
    hyper::{Body, Response},
    Filter, Rejection, Reply,
  };

  /// Create multipart upload
  #[utoipa::path(
    post,
    context_path = "/multipart-upload",
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
      .and(warp::any().map(move || s3_configuration.clone()))
      .and_then(
        |parameters: CreateUploadQueryParameters, s3_configuration: S3Configuration| async move {
          handle_create_multipart_upload(&s3_configuration, parameters.bucket, parameters.path)
            .await
        },
      )
  }

  async fn handle_create_multipart_upload(
    s3_configuration: &S3Configuration,
    bucket: String,
    key: String,
  ) -> Result<Response<Body>, Rejection> {
    log::info!("Create multipart upload...");
    let client = S3Client::try_from(s3_configuration)?;
    client
      .execute(|client: rusoto_s3::S3Client| async move {
        let request = CreateMultipartUploadRequest {
          bucket,
          key,
          ..Default::default()
        };

        client
          .create_multipart_upload(request)
          .await
          .map_err(|error| warp::reject::custom(Error::MultipartUploadCreationError(error)))
          .and_then(|output| {
            output
              .upload_id
              .ok_or_else(|| {
                warp::reject::custom(Error::MultipartUploadError(
                  "Invalid multipart upload creation response".to_string(),
                ))
              })
              .and_then(|upload_id| {
                let body_response = CreateUploadResponse { upload_id };
                to_ok_json_response(&body_response)
              })
          })
      })
      .await
  }
}
