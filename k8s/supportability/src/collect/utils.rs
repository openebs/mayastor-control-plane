use once_cell::sync::OnceCell;
use std::{fs::File, io::Write, path::PathBuf};

/// TOOL LOG FILE is the file that stores the logs of the support tool
static TOOL_LOG_FILE: OnceCell<File> = OnceCell::new();

/// Method to be only used to print tool logs to console and write in file
pub(crate) fn log(content: String) -> Result<(), std::io::Error> {
    println!("{}", content);
    write_to_log_file(content)?;
    Ok(())
}

/// Method to be only used to write in file
pub(crate) fn write_to_log_file(content: String) -> Result<(), std::io::Error> {
    TOOL_LOG_FILE
        .get()
        .expect("TOOL_LOG_FILE should have been initialised")
        .write_all(content.as_bytes())?;
    Ok(())
}

/// Method to initialise the TOOL_LOG_FILE once cell with a File
pub(crate) fn init_tool_log_file(file_path: PathBuf) -> Result<(), std::io::Error> {
    TOOL_LOG_FILE
        .set(File::create(file_path)?)
        .expect("Expect to be initialised only once");
    Ok(())
}

/// Flush the stream
pub(crate) fn flush_tool_log_file() -> Result<(), std::io::Error> {
    TOOL_LOG_FILE
        .get()
        .expect("TOOL_LOG_FILE should have been initialised")
        .flush()?;
    Ok(())
}
