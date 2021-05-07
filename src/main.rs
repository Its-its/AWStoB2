#![allow(clippy::await_holding_lock)]

#[macro_use] extern crate serde_derive;

use std::rc::Rc;
use std::sync::Mutex;

use futures::StreamExt;
use tokio::{
	fs::{OpenOptions, File},
	io::{
		AsyncBufReadExt, AsyncWriteExt, AsyncReadExt, BufReader, BufWriter
	}
};

use rusoto_core::Region;
use rusoto_s3::{GetObjectRequest, S3, S3Client};

mod backblaze;
pub mod error;

pub use error::Result;

use backblaze::{BlazeCache, UploadUrlResponse, get_or_update_blaze_cache};


pub static AWS_BUCKET_NAME: &str = "";
pub static AWS_REGION: Region = Region::UsEast1;
pub static BLAZE_BUCKET_ID: &str = "";
pub static BLAZE_CRED_ID: &str = "";
pub static BLAZE_CRED_KEY: &str = "";


pub mod aws_cache;
pub use aws_cache::run_aws_cache;


#[derive(Clone)]
pub struct BlazeUploadUrlCache(Rc<Mutex<Vec<UploadUrlResponse>>>);

impl BlazeUploadUrlCache {
	pub async fn new(blaze: &BlazeCache) -> Result<Self> {
		let mut urls: Vec<UploadUrlResponse> = Vec::new();

		for _ in 0..20 {
			urls.push(Self::new_upload_url(blaze).await?);
		}

		Ok(Self(Rc::new(Mutex::new(urls))))
	}

	pub async fn take_upload_url(&mut self, blaze: &BlazeCache) -> Result<UploadUrlResponse> {
		let mut lock = self.0.lock().map_err(|_| error::Error::PoisonError)?;

		if let Some(value) = lock.pop() {
			Ok(value)
		} else {
			Ok(Self::new_upload_url(blaze).await?)
		}
	}

	pub fn return_upload_url(&mut self, url: UploadUrlResponse) -> Result<()> {
		self.0.lock().map_err(|_| error::Error::PoisonError)?.push(url);
		Ok(())
	}

	pub async fn new_upload_url(blaze: &BlazeCache) -> Result<UploadUrlResponse> {
		blaze.auth.get_upload_url(BLAZE_BUCKET_ID).await
	}
}


#[derive(Clone)]
pub struct FailedTransfers(Rc<Mutex<BufWriter<File>>>);

impl FailedTransfers {
	pub async fn new() -> Result<Self> {
		let cache = OpenOptions::new().write(true).create(true).open(".failed_transfers").await?;
		Ok(Self(Rc::new(Mutex::new(BufWriter::new(cache)))))
	}

	pub async fn add_url_to_failed(&self, file_url: String) -> Result<()> {
		let mut lock = self.0.lock().map_err(|_| error::Error::PoisonError)?;

		let mut bytes = file_url.into_bytes();
		bytes.push(b'\n');

		lock.write_all(&bytes).await?;
		lock.flush().await?;

		Ok(())
	}
}


pub struct TransferToBlaze {
	file_path: String,
	aws: S3Client,
	blaze: BlazeCache,
	url_cache: BlazeUploadUrlCache
}

impl TransferToBlaze {
	pub async fn new(file_path: String, blaze: BlazeCache, url_cache: BlazeUploadUrlCache) -> Result<Self> {
		Ok(TransferToBlaze {
			file_path,
			blaze,
			url_cache,
			aws: S3Client::new(AWS_REGION.clone())
		})
	}

	pub async fn download_and_send_next(mut self) -> Result<()> {
		// let list = self.blaze.auth.list_file_names(&self.file_path, BLAZE_BUCKET_ID).await?;

		// if !list.files.is_empty() {
		// 	return Ok(());
		// }

		println!("Upload: {:?}", self.file_path);

		let obj = self.aws.get_object(GetObjectRequest {
			bucket: AWS_BUCKET_NAME.to_string(),
			key: self.file_path.clone(),

			.. Default::default()
		}).await.map_err(|_| error::Error::RusotoError)?;

		if let Some(content) = obj.body {
			let mut async_read = content.into_async_read();

			let mut image = Vec::new();
			async_read.read_to_end(&mut image).await?;

			let mut upload_url = self.url_cache.take_upload_url(&self.blaze).await?;

			if let Err(error::Error::Blaze(e)) = self.blaze.auth.upload_file(&upload_url, self.file_path.clone(), image.clone()).await {
				if matches!(e.status, 401 | 503) {
					upload_url = BlazeUploadUrlCache::new_upload_url(&self.blaze).await?;
					self.blaze.auth.upload_file(&upload_url, self.file_path, image).await?;
				} else {
					return Err(e.into());
				}
			}

			self.url_cache.return_upload_url(upload_url)?;
		} else {
			println!("No Body for {}", self.file_path);
		}

		Ok(())
	}
}


async fn run_transfer(file_path: String, blaze: BlazeCache, url_cache: BlazeUploadUrlCache) -> Result<()> {
	TransferToBlaze::new(file_path, blaze, url_cache)
	.await?
	.download_and_send_next()
	.await?;

	Ok(())
}


#[tokio::main]
async fn main() -> Result<()> {
	println!("Starting");

	// 1st step. Creates .aws_file
	run_aws_cache().await?;


	// Send to Backblaze
	let blaze = get_or_update_blaze_cache().await?;

	let failed_urls = FailedTransfers::new().await?;

	let upload_urls = BlazeUploadUrlCache::new(&blaze).await?;

	let cache = File::open(".aws_files").await?;
	let reader = BufReader::new(cache);

	let mut lines = Vec::new();

	let mut reader_lines = reader.lines();

	while let Some(line) = reader_lines.next_line().await? {
		lines.push(line.trim().to_string());
	}

	let uploads = futures::stream::iter(
		lines.into_iter()
		.map(|file_path| {
			let blaze = blaze.clone();
			let url_cache = upload_urls.clone();
			let failed = failed_urls.clone();

			async move {
				if let Err(e) = run_transfer(file_path.clone(), blaze, url_cache).await {
					eprintln!("Transfer Failed: {:?}", e);

					if let Err(e) = failed.add_url_to_failed(file_path).await {
						eprintln!("File Failed: {:?}", e);
					}
				}
			}
		})
	).buffer_unordered(10).collect::<Vec<()>>();

	uploads.await;

	println!("Finished Download/Upload");

	Ok(())
}