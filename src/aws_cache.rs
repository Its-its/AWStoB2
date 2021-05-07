use std::io::Write;
use std::fs::OpenOptions;

use rusoto_core::Region;
use rusoto_s3::{Object, ListObjectsV2Request, S3, S3Client};

use crate::{Result, AWS_BUCKET_NAME};
use crate::backblaze::{BlazeCache, get_or_update_blaze_cache};



// Transfer File Names
pub struct AWSTransferCache {
	aws: S3Client,
	next_continuation_token: Option<String>,
	cache: Vec<Object>
}

impl AWSTransferCache {
	async fn req_more_objects(&mut self) -> Result<()> {
		let objs = self.aws.list_objects_v2(ListObjectsV2Request {
			bucket: String::from(AWS_BUCKET_NAME),
			continuation_token: self.next_continuation_token.clone(),
			// max_keys: Some(5),

			.. Default::default()
		}).await;

		let objs = match objs {
			Ok(v) => v,
			Err(e) => {
				eprintln!("{:?}", e);
				return Ok(());
			}
		};

		self.next_continuation_token = objs.next_continuation_token;
		self.cache = objs.contents.unwrap_or_default();
		self.cache.reverse();

		Ok(())
	}

	pub async fn get_next_object(&mut self) -> Result<Option<Object>> {
		if self.cache.is_empty() && self.next_continuation_token.is_some() {
			tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

			self.req_more_objects().await?;
		}

		Ok(self.cache.pop())
	}
}

impl Default for AWSTransferCache {
	fn default() -> Self {
		AWSTransferCache {
			aws: S3Client::new(Region::UsEast1),
			next_continuation_token: None,
			cache: Vec::new()
		}
	}
}


#[derive(Clone)]
pub struct Processor {
	blaze: BlazeCache
}

impl Processor {
	pub async fn new() -> Result<Self> {
		Ok(Processor {
			blaze: get_or_update_blaze_cache().await?
		})
	}
}

/// Creates .aws_files
pub async fn run_aws_cache() -> Result<()> {
	let start_time = std::time::Instant::now();

	let mut file_cache = OpenOptions::new().append(true).create(true).open(".aws_files")?;

	let mut aws_cache = AWSTransferCache::default();
	aws_cache.req_more_objects().await?;

	let mut pos = 0;

	while let Some(object) = aws_cache.get_next_object().await? {
		pos += 1;

		if pos % 1000 == 0 {
			println!("POS: {}", pos);
		}

		if object.size.unwrap_or_default() > 0 && object.key.is_some() {
			let mut bytes = object.key.unwrap().into_bytes();
			bytes.push(b'\n');

			file_cache.write_all(&bytes)?;
			file_cache.flush()?;
		}
	}

	file_cache.sync_all()?;

	println!("Finished AWS Caching. Took: {:?}", start_time.elapsed());

	Ok(())
}