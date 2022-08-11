use crate::collect::error::Error;
use chrono::Utc;
use flate2::{write::GzEncoder, Compression};
use std::fs::File;
use tar::Builder;

// Holds prefix of archive file name
const ARCHIVE_PREFIX: &str = "mayastor";

/// Archive is a wrapper around tar::Writer to create archive files
pub(crate) struct Archive {
    tar_writer: Option<Builder<GzEncoder<File>>>,
}

impl Archive {
    /// Creates new archive file with 'mayastor-<timestamp>.tar.gz' in provided directory
    pub(crate) fn new(dir_path: Option<String>) -> Result<Self, Error> {
        let tar = if let Some(dir_path) = dir_path {
            let date = Utc::now();
            let archive_file_name = format!(
                "{}-{}.tar.gz",
                ARCHIVE_PREFIX,
                date.format("%Y-%m-%d--%H-%M-%S-%Z")
            );
            let tar_file_name = std::path::Path::new(&dir_path).join(archive_file_name);
            let tar_file = File::create(tar_file_name)?;
            let tar_gz = GzEncoder::new(tar_file, Compression::default());
            Some(Builder::new(tar_gz))
        } else {
            None
        };
        Ok(Self { tar_writer: tar })
    }

    /// Copies source directory & it's contents recursively into destination
    /// directory of archive file
    pub(crate) fn copy_to_archive(
        &mut self,
        src_dir: String,
        dest_dir: String,
    ) -> Result<(), std::io::Error> {
        if let Some(tar_writer) = self.tar_writer.as_mut() {
            tar_writer.append_dir_all(dest_dir, src_dir)?;
            tar_writer.finish()?;
        }
        Ok(())
    }
}
