use std::{
	fmt,
	sync::LockResult,
	io::Error as IoError
};

use reqwest::Error as HttpError;
use serde_json::Error as JsonError;

use crate::backblaze::BlazeError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
	Text(String),
	Blaze(BlazeError),

	Json(JsonError),
	Io(IoError),
	Request(HttpError),

	PoisonError,
	RusotoError
}


impl fmt::Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Error::Blaze(e) => write!(f, "Blaze Error: {:?}", e),
			Error::Text(e) => write!(f, "Custom Error: {:?}", e),
			Error::Json(e) => write!(f, "JSON Error: {:?}", e),
			Error::Io(e) => write!(f, "IO Error: {:?}", e),
			Error::Request(e) => write!(f, "Request Error: {:?}", e),
			Error::PoisonError => write!(f, "Posion Error"),
			Error::RusotoError => write!(f, "Rusoto Error")
		}
	}
}

impl From<IoError> for Error {
	fn from(error: IoError) -> Self {
		Self::Io(error)
	}
}

impl From<BlazeError> for Error {
	fn from(error: BlazeError) -> Self {
		Self::Blaze(error)
	}
}

impl From<HttpError> for Error {
	fn from(error: HttpError) -> Self {
		Self::Request(error)
	}
}

impl From<JsonError> for Error {
	fn from(error: JsonError) -> Self {
		Self::Json(error)
	}
}


impl From<String> for Error {
	fn from(error: String) -> Self {
		Self::Text(error)
	}
}

impl From<&str> for Error {
	fn from(error: &str) -> Self {
		Self::Text(error.to_string())
	}
}

impl<ZZZZ> From<LockResult<ZZZZ>> for Error {
	fn from(_: LockResult<ZZZZ>) -> Self {
		Self::PoisonError
	}
}