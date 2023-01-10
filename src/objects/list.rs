use crate::{s3_configuration::S3Configuration, to_ok_json_response};
use rusoto_credential::{AwsCredentials, StaticProvider};
use rusoto_s3::{ListObjectsV2Request, S3Client, S3};
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use warp::{
  hyper::{Body, Response},
  Filter, Rejection, Reply,
};

#[derive(Debug, Clone, Deserialize)]
struct ListObjectsQueryParameters {
  bucket: String,
  prefix: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ListObjectsResponse {
  #[serde(flatten)]
  objects: Vec<Object>,
}

pub(crate) fn route(
  s3_configuration: &S3Configuration,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
  let s3_configuration = s3_configuration.clone();
  warp::path("objects")
    .and(warp::get())
    .and(warp::query::<ListObjectsQueryParameters>())
    .map(move |parameters: ListObjectsQueryParameters| (parameters, s3_configuration.clone()))
    .and_then(
      |(parameters, s3_configuration): (ListObjectsQueryParameters, S3Configuration)| async move {
        handle_list_objects_signed_url(s3_configuration, parameters.bucket, parameters.prefix).await
      },
    )
}

async fn handle_list_objects_signed_url(
  s3_configuration: S3Configuration,
  bucket: String,
  source_prefix: Option<String>,
) -> Result<Response<Body>, Infallible> {
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
        .filter_map(|content| Object::build(&content.key, &source_prefix, false))
        .collect::<Vec<_>>()
    })
    .unwrap_or_default();

  let mut folders = response
    .common_prefixes
    .map(|prefixes| {
      prefixes
        .iter()
        .filter_map(|prefix| Object::build(&prefix.prefix, &source_prefix, true))
        .collect::<Vec<_>>()
    })
    .unwrap_or_default();

  objects.append(&mut folders);

  let response = ListObjectsResponse { objects };

  Ok(to_ok_json_response(&response))
}

#[derive(Debug, Serialize)]
struct Object {
  path: String,
  is_dir: bool,
}

impl Object {
  fn build(path: &Option<String>, prefix: &Option<String>, is_dir: bool) -> Option<Self> {
    let prefix_len = prefix.as_ref().map(|s| s.len()).unwrap_or(0);
    let path = path.clone().unwrap_or_default().split_off(prefix_len);

    if path.is_empty() {
      return None;
    }

    Some(Self { path, is_dir })
  }
}
