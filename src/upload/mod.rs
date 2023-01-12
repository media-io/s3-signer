pub(crate) mod abort_or_complete;
pub(crate) mod create;
pub(crate) mod part_upload_url;

use crate::{Error, S3Configuration};
use std::convert::TryFrom;
use warp::{hyper, Filter, Rejection, Reply};

pub(crate) fn routes(
  s3_configuration: &S3Configuration,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
  warp::path("multipart-upload").and(
    create::route(s3_configuration)
      .or(part_upload_url::route(s3_configuration))
      .or(abort_or_complete::route(s3_configuration)),
  )
}

struct S3Client {
  client: rusoto_s3::S3Client,
}

impl TryFrom<&S3Configuration> for S3Client {
  type Error = Rejection;

  fn try_from(s3_configuration: &S3Configuration) -> Result<Self, Self::Error> {
    let client = rusoto_s3::S3Client::try_from(s3_configuration)
      .map_err(|error| warp::reject::custom(Error::S3ConnectionError(error)))?;
    Ok(Self { client })
  }
}

impl S3Client {
  pub async fn execute<F, Fut>(
    self,
    operation: F,
  ) -> Result<hyper::Response<hyper::Body>, Rejection>
  where
    F: FnOnce(rusoto_s3::S3Client) -> Fut,
    Fut: std::future::Future<Output = Result<hyper::Response<hyper::Body>, Rejection>>,
  {
    operation(self.client).await
  }
}
