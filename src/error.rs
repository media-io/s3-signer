use rusoto_core::{request::TlsError, RusotoError};
use rusoto_s3::{
  AbortMultipartUploadError, CompleteMultipartUploadError, CreateMultipartUploadError,
  ListObjectsV2Error,
};
use std::fmt::{Debug, Display, Formatter};
use warp::{http::uri::InvalidUri, reject::Reject};

pub enum Error {
  HttpError(warp::http::Error),
  JsonError(serde_json::Error),
  ListObjectsError(RusotoError<ListObjectsV2Error>),
  MultipartUploadError(String),
  MultipartUploadAbortionError(RusotoError<AbortMultipartUploadError>),
  MultipartUploadCompletionError(RusotoError<CompleteMultipartUploadError>),
  MultipartUploadCreationError(RusotoError<CreateMultipartUploadError>),
  S3ConnectionError(TlsError),
  SignatureError(String),
  UriError(InvalidUri),
}

impl Debug for Error {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    match self {
      Error::HttpError(error) => {
        write!(f, "HTTP: {:?}", error)
      }
      Error::JsonError(error) => {
        write!(f, "JSON: {:?}", error)
      }
      Error::ListObjectsError(error) => {
        write!(f, "Objects listing: {:?}", error)
      }
      Error::MultipartUploadAbortionError(error) => {
        write!(f, "Multipart upload abortion: {:?}", error)
      }
      Error::MultipartUploadCompletionError(error) => {
        write!(f, "Multipart upload completion: {:?}", error)
      }
      Error::MultipartUploadCreationError(error) => {
        write!(f, "Multipart upload creation: {:?}", error)
      }
      Error::MultipartUploadError(error) => write!(f, "Multipart upload: {:?}", error),
      Error::S3ConnectionError(error) => write!(f, "Cannot create S3 client: {:?}", error),
      Error::SignatureError(error) => write!(f, "Signature: {:?}", error),
      Error::UriError(error) => {
        write!(f, "URI: {:?}", error)
      }
    }
  }
}

impl Display for Error {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(f, "{:?}", self)
  }
}

impl std::error::Error for Error {}

impl Reject for Error {}
