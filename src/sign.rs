use crate::{to_ok_json_response, S3Configuration};
use http_types::{Method, Request, Response, StatusCode};
use rusoto_credential::{AwsCredentials, StaticProvider};
use rusoto_s3::{
  util::{PreSignedRequest, PreSignedRequestOption},
  GetObjectRequest, ListObjectsV2Request, PutObjectRequest, S3Client, S3,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct SignQueryParameters {
  bucket: String,
  path: String,
  #[serde(default = "default_as_false")]
  list: bool,
  #[serde(default = "default_as_false")]
  create: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct PresignedUrlResponse {
  pub(crate) url: String,
}

fn default_as_false() -> bool {
  false
}

pub(crate) fn handle_signature_request(
  request: &Request,
  s3_configuration: &S3Configuration,
) -> http_types::Result<Response> {
  if request.method() == Method::Options {
    let mut response = Response::new(StatusCode::Ok);
    response.insert_header("Allow", "GET, OPTIONS, HEAD");
    response.insert_header("Access-Control-Allow-Origin", "*");
    response.insert_header(
      "Access-Control-Allow-Methods",
      "GET, POST, PUT, DELETE, PATCH, OPTIONS",
    );
    response.insert_header("Access-Control-Allow-Headers", "*");
    return Ok(response);
  }

  if let Ok(SignQueryParameters {
    bucket,
    path,
    list,
    create,
  }) = request.query()
  {
    let credentials = AwsCredentials::from(s3_configuration);

    if list {
      let result = list_directory(s3_configuration, &bucket, Some(path));

      let mut response = Response::new(StatusCode::Ok);
      response.insert_header("Content-Type", "application/json");
      response.set_body(result);
      return Ok(response);
    }

    let presigned_url = if create {
      let put_object = PutObjectRequest {
        bucket,
        key: path,
        ..Default::default()
      };

      put_object.get_presigned_url(
        &s3_configuration.s3_region,
        &credentials,
        &PreSignedRequestOption::default(),
      )
    } else {
      let get_object = GetObjectRequest {
        bucket,
        key: path,
        ..Default::default()
      };

      get_object.get_presigned_url(
        &s3_configuration.s3_region,
        &credentials,
        &PreSignedRequestOption::default(),
      )
    };

    let body_response = PresignedUrlResponse { url: presigned_url };

    Ok(to_ok_json_response(&body_response))
  } else {
    Ok(Response::new(StatusCode::UnprocessableEntity))
  }
}

#[derive(Debug, Serialize)]
struct Object {
  path: String,
  is_dir: bool,
}

fn list_directory(
  s3_configuration: &S3Configuration,
  bucket: &str,
  source_prefix: Option<String>,
) -> String {
  use tokio::runtime::Runtime;

  let runtime = Runtime::new().unwrap();

  runtime.block_on(async {
    let credentials = AwsCredentials::new(
      &s3_configuration.s3_access_key_id,
      &s3_configuration.s3_secret_access_key,
      None,
      None,
    );

    let list_objects = ListObjectsV2Request {
      bucket: bucket.to_string(),
      delimiter: Some(String::from("/")),
      prefix: source_prefix.clone(),
      ..Default::default()
    };

    let http_client = rusoto_core::request::HttpClient::new().unwrap();
    let credentials: StaticProvider = credentials.into();

    let client = S3Client::new_with(http_client, credentials, s3_configuration.s3_region.clone());

    let response = client.list_objects_v2(list_objects).await.unwrap();

    let mut objects = response
      .contents
      .map(|contents| {
        contents
          .iter()
          .filter_map(|content| build_object(&content.key, &source_prefix, false))
          .collect::<Vec<_>>()
      })
      .unwrap_or_default();

    let mut folders = response
      .common_prefixes
      .map(|prefixes| {
        prefixes
          .iter()
          .filter_map(|prefix| build_object(&prefix.prefix, &source_prefix, true))
          .collect::<Vec<_>>()
      })
      .unwrap_or_default();

    objects.append(&mut folders);

    serde_json::to_string(&objects).unwrap()
  })
}

fn build_object(path: &Option<String>, prefix: &Option<String>, is_dir: bool) -> Option<Object> {
  let prefix_len = prefix.as_ref().map(|s| s.len()).unwrap_or(0);
  let path = path.clone().unwrap_or_default().split_off(prefix_len);

  if path.is_empty() {
    return None;
  }

  Some(Object { path, is_dir })
}
