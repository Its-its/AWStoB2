use std::{
	io::{
		Seek, SeekFrom, Write, BufReader
	},
	time::{
		Duration, SystemTime
	},
	fs::OpenOptions
};

use crypto::sha1::Sha1;
use crypto::digest::Digest;
use base64::encode as b64encode;
use reqwest::Client;

use crate::Result;



// const API_URL_V1: &str = "https://api.backblazeb2.com/b2api/v1";
const API_URL_V2: &str = "https://api.backblazeb2.com/b2api/v2";
// const API_URL_V3: &str = "https://api.backblazeb2.com/b2api/v3";
// const API_URL_V4: &str = "https://api.backblazeb2.com/b2api/v4";
// const API_URL_V5: &str = "https://api.backblazeb2.com/b2api/v5";



#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlazeError {
	pub status: i64,
	pub code: String,
	pub message: String
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlazeCache {
	// since epoch (seconds)
	pub last_updated: u64,

	pub auth: B2Authorization
}


pub async fn get_or_update_blaze_cache() -> Result<BlazeCache> {
	let file_path = ".blaze_cache";

	let curr_update = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();

	{ // Read from file.
		let file = OpenOptions::new()
			.read(true)
			.create(true)
			.write(true)
			.open(file_path)?;

		let buf_reader = BufReader::new(file);

		if let Ok(cache) = serde_json::from_reader::<_, BlazeCache>(buf_reader) {
			let cached_length = curr_update - Duration::from_secs(cache.last_updated);

			println!("Cached Since: {:?}", cached_length);

			if cached_length <= Duration::from_secs(60 * 60 * 24) {
				println!("Using Cache");
				return Ok(cache);
			}
		}
	}

	println!("Writing to Cache");

	let creds = Credentials::new(crate::BLAZE_CRED_ID, crate::BLAZE_CRED_KEY);
	let auth = creds.authorize().await?;

	let cache = BlazeCache { auth, last_updated: curr_update.as_secs() };


	let mut file = OpenOptions::new().truncate(true).write(true).create(true).open(file_path)?;

	file.seek(SeekFrom::Start(0))?;
	file.write_all(serde_json::to_string(&cache).unwrap().as_bytes())?;

	Ok(cache)
}



pub struct Credentials {
	pub id: String,
	pub key: String
}

impl Credentials {
	pub fn new<S: Into<String>>(id: S, key: S) -> Self {
		Self {
			id: id.into(),
			key: key.into()
		}
	}

	fn header_name<'a>(&self) -> &'a str {
		"Authorization"
	}

	fn id_key(&self) -> String {
		format!("{}:{}", self.id, self.key)
	}

	pub fn auth_string(&self) -> String {
		format!("Basic {}", b64encode(&self.id_key()))
	}

	pub async fn authorize(&self) -> Result<B2Authorization> {
		let client = Client::new();

		let resp = client.get(format!("{}/b2_authorize_account", API_URL_V2).as_str())
			.header(self.header_name(), self.auth_string())
			.send()
			.await?;

		if resp.status().is_success() {
			Ok(B2Authorization::new(self.id.clone(), resp.json().await?))
		} else {
			println!("{:#?}", resp.text().await?);
			Err("Authorization Error".into())
		}
	}
}


#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct B2AuthResponse {
	// account_id: String,
	// allowed: Object,
	absolute_minimum_part_size: usize,
	api_url: String,
	authorization_token: String,
	download_url: String,
	recommended_part_size: usize,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct B2Authorization {
	pub account_id: String,
	pub authorization_token: String,
	pub api_url: String,
	pub download_url: String,
	pub recommended_part_size: usize,
	pub absolute_minimum_part_size: usize
}

impl B2Authorization {
	fn new(id: String, resp: B2AuthResponse) -> B2Authorization {
		B2Authorization {
			account_id: id,
			authorization_token: resp.authorization_token,
			api_url: resp.api_url,
			download_url: resp.download_url,
			recommended_part_size: resp.recommended_part_size,
			absolute_minimum_part_size: resp.absolute_minimum_part_size
		}
	}

	pub async fn get_upload_url(&self, bucket_id: &str) -> Result<UploadUrlResponse> {
		let client = Client::new();

		let body = serde_json::json!({
			"bucketId": bucket_id
		});

		let resp = client.post(format!("{}/b2api/v2/b2_get_upload_url", self.api_url).as_str())
			.header("Authorization", self.authorization_token.as_str())
			.body(serde_json::to_string(&body)?)
			.send()
			.await?;

		if resp.status().is_success() {
			Ok(resp.json().await?)
		} else {
			Err(resp.text().await?.into())
		}
	}

	pub async fn list_file_names(&self, prefix: &str, bucket_id: &str) -> Result<ListFileNamesResponse> {
		let client = Client::new();

		let body = serde_json::json!({
			"bucketId": bucket_id,
			"prefix": prefix
		});

		let resp = client.post(format!("{}/b2api/v2/b2_list_file_names", self.api_url).as_str())
			.header("Authorization", self.authorization_token.as_str())
			.body(serde_json::to_string(&body)?)
			.send()
			.await?;

		if resp.status().is_success() {
			Ok(resp.json().await?)
		} else {
			Err(resp.text().await?.into())
		}
	}

	pub async fn upload_file(&self, upload: &UploadUrlResponse, file_name: String, image: Vec<u8>) -> Result<serde_json::Value> {
		let client = Client::new();

		let mut sha = Sha1::new();
		sha.input(image.as_ref());
		let sha = sha.result_str();

		// println!("Size: {}", image.len());
		// println!("Sha1: {}", sha);

		let resp = client.post(upload.upload_url.as_str())
			.header("Authorization", upload.authorization_token.as_str())
			.header("Content-Type", "b2/x-auto")
			.header("Content-Length", image.len())
			.header("X-Bz-File-Name", encode_file_name(file_name).as_str())
			.header("X-Bz-Content-Sha1", sha.as_str())
			.body(image)
			.send()
			.await?;

		if resp.status().is_success() {
			Ok(resp.json().await?)
		} else {
			Err(crate::error::Error::Blaze(resp.json().await?))
		}
	}

	// ^ Returns.
	// Object({
	// 	"accountId": String(
	// 	),
	// 	"action": String(
	// 	),
	// 	"bucketId": String(
	// 	),
	// 	"contentLength": Number(
	// 	),
	// 	"contentMd5": String(
	// 	),
	// 	"contentSha1": String(
	// 	),
	// 	"contentType": String(
	// 	),
	// 	"fileId": String(
	// 	),
	// 	"fileInfo": Object({}),
	// 	"fileName": String(
	// 	),
	// 	"uploadTimestamp": Number(
	// 	),
	// })
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadUrlResponse {
	pub authorization_token: String,
	pub bucket_id: String,
	pub upload_url: String
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListFileNamesResponse {
	pub files: Vec<FileName>,
	pub next_file_name: Option<String>
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileName {
	account_id: String,
	action: String,
	bucket_id: String,
	content_length: i64,
	content_sha1: String,
	content_md5: String,
	content_type: String,
	file_id: String,
	file_name: String,
	upload_timestamp: i64
	// fileInfo
}


// Names can be pretty much any UTF-8 string up to 1024 bytes long. There are a few picky rules:
// No character codes below 32 are allowed.
// Backslashes are not allowed.
// DEL characters (127) are not allowed.
// File names cannot start with "/", end with "/", or contain "//".

pub fn encode_file_name(file_name: String) -> String {
	file_name.replace("\\", "-")
	.replace(" ", "%20")
}