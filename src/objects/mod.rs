#[cfg(feature = "server")]
pub(crate) mod create;
#[cfg(feature = "server")]
pub(crate) mod get;
pub(crate) mod list;

pub use list::{ListObjectsQueryParameters, ListObjectsResponse, Object};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SignQueryParameters {
  pub bucket: String,
  pub path: String,
}

#[cfg(feature = "server")]
pub(crate) use server::routes;

#[cfg(feature = "server")]
mod server {
  use super::*;
  use crate::S3Configuration;
  use warp::{Filter, Rejection, Reply};

  pub(crate) fn routes(
    s3_configuration: &S3Configuration,
  ) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    get::route(s3_configuration)
      .or(create::route(s3_configuration))
      .or(list::server::route(s3_configuration))
  }
}
