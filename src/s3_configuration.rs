use rusoto_core::{request::TlsError, HttpClient};
use rusoto_credential::{AwsCredentials, StaticProvider};
use rusoto_s3::S3Client;
use rusoto_signature::Region;
use std::convert::TryFrom;

#[derive(Clone, Debug)]
pub(crate) struct S3Configuration {
  pub(crate) s3_access_key_id: String,
  pub(crate) s3_secret_access_key: String,
  pub(crate) s3_region: Region,
}

impl From<&S3Configuration> for AwsCredentials {
  fn from(s3_configuration: &S3Configuration) -> Self {
    Self::new(
      &s3_configuration.s3_access_key_id,
      &s3_configuration.s3_secret_access_key,
      None,
      None,
    )
  }
}

impl TryFrom<&S3Configuration> for S3Client {
  type Error = TlsError;

  fn try_from(s3_configuration: &S3Configuration) -> Result<Self, Self::Error> {
    let http_client = HttpClient::new()?;
    let client = S3Client::new_with(
      http_client,
      StaticProvider::new_minimal(
        s3_configuration.s3_access_key_id.clone(),
        s3_configuration.s3_secret_access_key.clone(),
      ),
      s3_configuration.s3_region.clone(),
    );

    Ok(client)
  }
}
