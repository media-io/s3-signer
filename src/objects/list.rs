use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ListObjectsQueryParameters {
  pub bucket: String,
  pub prefix: Option<String>,
}

pub type ListObjectsResponse = Vec<Object>;

#[derive(Debug, Deserialize, Serialize)]
#[cfg_attr(feature = "server", derive(utoipa::ToSchema))]
pub struct Object {
  pub path: String,
  pub is_dir: bool,
}

impl Object {
  pub fn build(path: &Option<String>, prefix: &Option<String>, is_dir: bool) -> Option<Self> {
    let prefix_len = prefix.as_ref().map(|s| s.len()).unwrap_or(0);
    let path = path.clone().unwrap_or_default().split_off(prefix_len);

    if path.is_empty() {
      return None;
    }

    Some(Self { path, is_dir })
  }
}

#[cfg(feature = "server")]
pub(crate) mod server {
  use super::*;
  use crate::{to_ok_json_response, Error, S3Configuration};
  use rusoto_credential::{AwsCredentials, StaticProvider};
  use rusoto_s3::{ListObjectsV2Request, S3Client, S3};
  use warp::{
    hyper::{Body, Response},
    Filter, Rejection, Reply,
  };

  /// List objects
  #[utoipa::path(
    get,
    path = "/objects",
    tag = "Objects",
    responses(
      (
        status = 200,
        description = "Successfully list objects",
        content_type = "application/json",
        body = ListObjectsResponse
      ),
    ),
    params(
      ("bucket" = String, Query, description = "Name of the bucket"),
      ("prefix" = Option<String>, Query, description = "Prefix to filter objects to list")
    ),
  )]
  pub(crate) fn route(
    s3_configuration: &S3Configuration,
  ) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let s3_configuration = s3_configuration.clone();
    warp::path("objects")
      .and(warp::get())
      .and(warp::query::<ListObjectsQueryParameters>())
      .and(warp::any().map(move || s3_configuration.clone()))
      .and_then(
        |parameters: ListObjectsQueryParameters, s3_configuration: S3Configuration| async move {
          handle_list_objects(s3_configuration, parameters.bucket, parameters.prefix).await
        },
      )
  }

  async fn handle_list_objects(
    s3_configuration: S3Configuration,
    bucket: String,
    source_prefix: Option<String>,
  ) -> Result<Response<Body>, Rejection> {
    log::info!(
      "List objects signed URL: bucket={}, source_prefix={:?}",
      bucket,
      source_prefix
    );
    let credentials = AwsCredentials::from(&s3_configuration);

    let list_objects = ListObjectsV2Request {
      bucket: bucket.to_string(),
      delimiter: Some(String::from("/")),
      prefix: source_prefix.clone(),
      ..Default::default()
    };

    let http_client = rusoto_core::request::HttpClient::new()
      .map_err(|error| warp::reject::custom(Error::S3ConnectionError(error)))?;
    let credentials: StaticProvider = credentials.into();

    let client = S3Client::new_with(http_client, credentials, s3_configuration.region().clone());

    let response = client
      .list_objects_v2(list_objects)
      .await
      .map_err(|error| warp::reject::custom(Error::ListObjectsError(error)))?;

    let mut objects = response
      .contents
      .map(|contents| {
        contents
          .iter()
          .filter_map(|content| Object::build(&content.key, &source_prefix, false))
          .collect::<ListObjectsResponse>()
      })
      .unwrap_or_default();

    let mut folders = response
      .common_prefixes
      .map(|prefixes| {
        prefixes
          .iter()
          .filter_map(|prefix| Object::build(&prefix.prefix, &source_prefix, true))
          .collect::<ListObjectsResponse>()
      })
      .unwrap_or_default();

    objects.append(&mut folders);

    to_ok_json_response(&objects)
  }
}
