use rusoto_core::{request::TlsError, HttpClient};
use rusoto_credential::{AwsCredentials, StaticProvider};
use rusoto_s3::S3Client;
use rusoto_signature::{region::ParseRegionError, Region};
use std::{convert::TryFrom, str::FromStr};

#[derive(Clone, Debug)]
pub struct S3Configuration {
  s3_access_key_id: String,
  s3_secret_access_key: String,
  s3_region: Region,
}

impl S3Configuration {
  pub fn new(
    access_key_id: &str,
    secret_access_key: &str,
    region: &str,
  ) -> Result<Self, ParseRegionError> {
    Region::from_str(region).map(|region| Self {
      s3_access_key_id: access_key_id.to_string(),
      s3_secret_access_key: secret_access_key.to_string(),
      s3_region: region,
    })
  }

  pub fn new_with_hostname(
    access_key_id: &str,
    secret_access_key: &str,
    region: &str,
    hostname: &str,
  ) -> Self {
    let region = Region::Custom {
      name: region.to_string(),
      endpoint: hostname.to_string(),
    };

    Self {
      s3_access_key_id: access_key_id.to_string(),
      s3_secret_access_key: secret_access_key.to_string(),
      s3_region: region,
    }
  }

  pub fn access_key_id(&self) -> &String {
    &self.s3_access_key_id
  }

  pub fn secret_access_key(&self) -> &String {
    &self.s3_secret_access_key
  }

  pub fn region(&self) -> &Region {
    &self.s3_region
  }
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
