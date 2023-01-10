pub(crate) mod create;
pub(crate) mod get;
pub(crate) mod list;

use crate::S3Configuration;
use serde::Deserialize;
use warp::{Filter, Rejection, Reply};

#[derive(Debug, Clone, Deserialize)]
struct SignQueryParameters {
  bucket: String,
  path: String,
}

pub(crate) fn routes(
  s3_configuration: &S3Configuration,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
  get::route(s3_configuration)
    .or(create::route(s3_configuration))
    .or(list::route(s3_configuration))
}
