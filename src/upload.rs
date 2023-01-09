use crate::{sign::PresignedUrlResponse, to_ok_json_response, S3Configuration};
use http_types::{Method, Request, Response, StatusCode};
use rusoto_credential::AwsCredentials;
use rusoto_s3::{
  util::{PreSignedRequest, PreSignedRequestOption},
  AbortMultipartUploadRequest, CompleteMultipartUploadRequest, CompletedMultipartUpload,
  CompletedPart, CreateMultipartUploadRequest, S3Client, UploadPartRequest, S3,
};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use tokio::runtime::Runtime;

#[derive(Debug, Deserialize)]
struct UploadQueryParameters {
  bucket: String,
  path: String,
  upload_id: Option<String>,
  part_number: Option<i64>,
}

#[derive(Debug, Serialize)]
struct CreateUploadResponse {
  upload_id: String,
}

pub(crate) fn handle_upload_request(
  request: &mut Request,
  s3_configuration: &S3Configuration,
) -> http_types::Result<Response> {
  match request.method() {
    Method::Post => {
      if let Ok(UploadQueryParameters {
        bucket,
        path,
        upload_id,
        ..
      }) = request.query()
      {
        return if let Some(upload_id) = upload_id {
          handle_complete_multipart_upload(s3_configuration, request, &bucket, &path, &upload_id)
        } else {
          handle_create_multipart_upload(s3_configuration, &bucket, &path)
        };
      }
    }
    Method::Get => {
      if let Ok(UploadQueryParameters {
        bucket,
        path,
        upload_id: Some(upload_id),
        part_number: Some(part_number),
      }) = request.query()
      {
        return handle_part_upload_presigned_url(
          s3_configuration,
          &bucket,
          &path,
          &upload_id,
          part_number,
        );
      }
    }
    Method::Delete => {
      if let Ok(UploadQueryParameters {
        bucket,
        path,
        upload_id: Some(upload_id),
        ..
      }) = request.query()
      {
        return handle_abort_multipart_upload(s3_configuration, &bucket, &path, &upload_id);
      }
    }
    _ => {}
  }

  Ok(Response::from(StatusCode::UnprocessableEntity))
}

fn handle_create_multipart_upload(
  s3_configuration: &S3Configuration,
  bucket: &str,
  path: &str,
) -> http_types::Result<Response> {
  log::info!("Create multipart upload...");
  execute_s3_request_operation(s3_configuration, |client| async move {
    let request = CreateMultipartUploadRequest {
      bucket: bucket.to_string(),
      key: path.to_string(),
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
    Ok(Response::from(StatusCode::InternalServerError))
  })
}

fn handle_part_upload_presigned_url(
  s3_configuration: &S3Configuration,
  bucket: &str,
  path: &str,
  upload_id: &str,
  part_number: i64,
) -> http_types::Result<Response> {
  log::info!(
    "Upload part: upload_id={}, part_number={}",
    upload_id,
    part_number,
  );
  let request = UploadPartRequest {
    bucket: bucket.to_string(),
    key: path.to_string(),
    upload_id: upload_id.to_string(),
    part_number,
    ..Default::default()
  };

  let credentials = AwsCredentials::from(s3_configuration);

  let presigned_url = request.get_presigned_url(
    &s3_configuration.s3_region,
    &credentials,
    &PreSignedRequestOption::default(),
  );

  let body_response = PresignedUrlResponse { url: presigned_url };
  Ok(to_ok_json_response(&body_response))
}

fn handle_complete_multipart_upload(
  s3_configuration: &S3Configuration,
  request: &mut Request,
  bucket: &str,
  path: &str,
  upload_id: &str,
) -> http_types::Result<Response> {
  log::info!("Complete multipart upload: upload_id={}", upload_id);
  execute_s3_request_operation(s3_configuration, |client| async move {
    let parts: Vec<(i64, String)> = request.body_json().await.unwrap();
    let parts = parts
      .into_iter()
      .map(|(part_number, e_tag)| CompletedPart {
        part_number: Some(part_number),
        e_tag: Some(e_tag),
      })
      .collect();
    let parts = CompletedMultipartUpload { parts: Some(parts) };

    let request = CompleteMultipartUploadRequest {
      bucket: bucket.to_string(),
      key: path.to_string(),
      upload_id: upload_id.to_string(),
      multipart_upload: Some(parts),
      ..Default::default()
    };

    if let Err(error) = client.complete_multipart_upload(request).await {
      log::error!("Failure on complete_multipart_upload: {}", error);
      Ok(Response::from(StatusCode::InternalServerError))
    } else {
      Ok(Response::from(StatusCode::Ok))
    }
  })
}

fn handle_abort_multipart_upload(
  s3_configuration: &S3Configuration,
  bucket: &str,
  path: &str,
  upload_id: &str,
) -> http_types::Result<Response> {
  log::info!("Abort multipart upload: upload_id={}", upload_id);
  execute_s3_request_operation(s3_configuration, |client| async move {
    let request = AbortMultipartUploadRequest {
      bucket: bucket.to_string(),
      key: path.to_string(),
      upload_id: upload_id.to_string(),
      ..Default::default()
    };

    if let Err(error) = client.abort_multipart_upload(request).await {
      log::error!("Failure on abort_multipart_upload: {}", error);
      Ok(Response::from(StatusCode::InternalServerError))
    } else {
      Ok(Response::from(StatusCode::Ok))
    }
  })
}

fn execute_s3_request_operation<F, Fut>(
  s3_configuration: &S3Configuration,
  operation: F,
) -> http_types::Result<Response>
where
  F: FnOnce(S3Client) -> Fut,
  Fut: std::future::Future<Output = http_types::Result<Response>>,
{
  let result = S3Client::try_from(s3_configuration);
  if let Err(error) = result {
    log::error!("Cannot create S3 client: {}", error);
    return Ok(Response::from(StatusCode::InternalServerError));
  }

  let client = result.unwrap();
  let runtime = Runtime::new().unwrap();

  runtime.block_on(async { operation(client).await })
}
