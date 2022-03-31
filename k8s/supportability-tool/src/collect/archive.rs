use crate::collect::error::Error;
use chrono::Local;
use flate2::{write::GzEncoder, Compression};
use std::fs::File;
use tar::Builder;

// Holds prefix of archive file name
const ARCHIVE_PREFIX: &str = "mayastor";

/// Archive is a wrapper around tar::Writer to create archive files
pub(crate) struct Archive {
    tar_writer: Builder<GzEncoder<File>>,
}

impl Archive {
    /// Creates new archive file with 'mayastor-<timestamp>.tar.gz' in provided directory
    pub(crate) fn new(dir_path: String) -> Result<Self, Error> {
        let date = Local::now();
        let archive_file_name = format!(
            "{}-{}.tar.gz",
            ARCHIVE_PREFIX,
            date.format("%Y-%m-%d-%H-%M-%S")
        );
        let tar_file_name = std::path::Path::new(&dir_path).join(archive_file_name);
        let tar_file = File::create(tar_file_name)?;
        let tar_gz = GzEncoder::new(tar_file, Compression::default());
        let tar = Builder::new(tar_gz);
        Ok(Self { tar_writer: tar })
    }

    /// Copies source directory & it's contents recursively into destination
    /// directory of archive file
    pub(crate) fn copy_to_archive(
        &mut self,
        src_dir: String,
        dest_dir: String,
    ) -> Result<(), std::io::Error> {
        self.tar_writer.append_dir_all(dest_dir, src_dir)?;
        self.tar_writer.finish()?;
        Ok(())
    }
}
