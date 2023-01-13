use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct PartUploadQueryParameters {
  pub bucket: String,
  pub path: String,
}

#[cfg(feature = "server")]
pub(crate) mod server {
  use super::PartUploadQueryParameters;
  use crate::{to_redirect_response, S3Configuration};
  use rusoto_credential::AwsCredentials;
  use rusoto_s3::{
    util::{PreSignedRequest, PreSignedRequestOption},
    UploadPartRequest,
  };
  use warp::{
    hyper::{Body, Response},
    Filter, Rejection, Reply,
  };

  /// Pre-sign part upload URL
  #[utoipa::path(
    put,
    context_path = "/multipart-upload",
    path = "/{upload_id}/part/{part_number}",
    tag = "Multipart upload",
    responses(
      (status = 302, description = "Redirect to pre-signed URL for getting an object"),
    ),
    params(
      ("upload_id" = String, Path, description = "ID of the upload"),
      ("part_number" = i64, Path, description = "Index number of the part to upload"),
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
      .and(warp::any().map(move || s3_configuration.clone()))
      .and_then(
        |upload_id: String,
         part_number: i64,
         parameters: PartUploadQueryParameters,
         s3_configuration: S3Configuration| async move {
          handle_part_upload_presigned_url(
            &s3_configuration,
            parameters.bucket,
            parameters.path,
            upload_id,
            part_number,
          )
          .await
        },
      )
  }

  async fn handle_part_upload_presigned_url(
    s3_configuration: &S3Configuration,
    bucket: String,
    key: String,
    upload_id: String,
    part_number: i64,
  ) -> Result<Response<Body>, Rejection> {
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

    let credentials = AwsCredentials::from(s3_configuration);

    let presigned_url = request.get_presigned_url(
      s3_configuration.region(),
      &credentials,
      &PreSignedRequestOption::default(),
    );

    to_redirect_response(&presigned_url)
  }
}
