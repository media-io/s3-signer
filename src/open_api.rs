use std::{collections::BTreeMap, str::FromStr, sync::Arc};
use utoipa::{
  openapi::{Components, OpenApiBuilder, PathItem, Paths},
  OpenApi,
};
use utoipa_swagger_ui::Config;
use warp::{
  hyper::{header::CONTENT_TYPE, Response, StatusCode, Uri},
  path::{FullPath, Tail},
  Filter, Rejection, Reply,
};

#[derive(OpenApi)]
#[openapi(
  paths(
    crate::objects::list::route,
    crate::objects::get::route,
    crate::objects::create::route,
    crate::upload::create::route,
    crate::upload::part_upload_url::route,
    crate::upload::abort_or_complete::route,
  ),
  components(
    schemas(
      crate::objects::list::Object,
      crate::upload::create::CreateUploadResponse,
      crate::upload::abort_or_complete::CompletedUploadPart,
      crate::upload::abort_or_complete::AbortOrCompleteUploadBody,
     )
  ),
  tags(
    (name = "Objects", description = "Objects-related API"),
    (name = "Multipart upload", description = "Multipart upload API")
  )
)]
struct ApiDoc;

pub fn swagger_route(
  path: &str,
  open_api_route: &str,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
  let open_api_route = format!("/{}", open_api_route.trim_start_matches('/'));
  let config = Arc::new(Config::from(open_api_route));

  let path = path
    .trim_start_matches('/')
    .trim_end_matches('/')
    .to_string();

  warp::path(path.clone())
    .and(warp::get())
    .and(warp::path::full())
    .and(warp::path::tail())
    .and(warp::any().map(move || config.clone()))
    .and(warp::any().map(move || path.clone()))
    .and_then(serve_swagger)
}

pub fn insert_open_api_at(
  base: utoipa::openapi::OpenApi,
  prefix_path: &str,
) -> utoipa::openapi::OpenApi {
  concat(base, ApiDoc::openapi(), prefix_path)
}

fn concat(
  base: utoipa::openapi::OpenApi,
  other: utoipa::openapi::OpenApi,
  prefix_path: &str,
) -> utoipa::openapi::OpenApi {
  OpenApiBuilder::new()
    .info(base.info)
    .paths(merge_paths(base.paths, other.paths, prefix_path))
    .components(merge_components(base.components, other.components))
    .servers(merge(base.servers, other.servers))
    .security(merge(base.security, other.security))
    .tags(merge(base.tags, other.tags))
    .build()
}

fn merge_paths(mut base: Paths, other: Paths, prefix_path: &str) -> Paths {
  let mut paths = Paths::new();
  let mut other_paths = other
    .paths
    .into_iter()
    .map(|(key, value)| {
      let new_key = format!(
        "/{}/{}",
        prefix_path.trim_start_matches('/').trim_end_matches('/'),
        key.trim_start_matches('/')
      );
      (new_key, value)
    })
    .collect::<BTreeMap<String, PathItem>>();

  paths.paths.append(&mut base.paths);
  paths.paths.append(&mut other_paths);

  paths
}

fn merge_components(a: Option<Components>, b: Option<Components>) -> Option<Components> {
  match (a, b) {
    (Some(mut a_components), Some(mut b_components)) => {
      for (k, v) in b_components.responses {
        a_components.responses.insert(k, v);
      }
      a_components.schemas.append(&mut b_components.schemas);
      a_components
        .security_schemes
        .append(&mut b_components.security_schemes);
      Some(a_components)
    }
    (Some(a_components), None) => Some(a_components),
    (None, Some(b_components)) => Some(b_components),
    (None, None) => None,
  }
}

fn merge<T>(a: Option<Vec<T>>, b: Option<Vec<T>>) -> Option<Vec<T>> {
  match (a, b) {
    (Some(mut a_values), Some(mut b_values)) => {
      a_values.append(&mut b_values);
      Some(a_values)
    }
    (Some(a_values), None) => Some(a_values),
    (None, Some(b_values)) => Some(b_values),
    (None, None) => None,
  }
}

async fn serve_swagger(
  full_path: FullPath,
  tail: Tail,
  config: Arc<Config<'static>>,
  path: String,
) -> Result<Box<dyn Reply + 'static>, Rejection> {
  let path = format!("/{}/", path);
  if full_path.as_str() == path.trim_end_matches('/') {
    return Ok(Box::new(warp::redirect::found(
      Uri::from_str(&path).unwrap(),
    )));
  }

  let path = tail.as_str();
  match utoipa_swagger_ui::serve(path, config) {
    Ok(file) => {
      if let Some(file) = file {
        Ok(Box::new(
          Response::builder()
            .header(CONTENT_TYPE, file.content_type)
            .body(file.bytes),
        ))
      } else {
        Ok(Box::new(StatusCode::NOT_FOUND))
      }
    }
    Err(error) => Ok(Box::new(
      Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(error.to_string()),
    )),
  }
}
