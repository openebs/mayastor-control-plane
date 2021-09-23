use std::{collections::HashMap, io::Write};

type BuildResult = Result<(), Box<dyn std::error::Error>>;

/// Various common constants used by the control plane
pub mod constants {
    include!("./src/constants.rs");
}

fn main() -> BuildResult {
    if !std::path::Path::new("../chart").exists() {
        return Ok(());
    }

    let path = std::path::Path::new("../chart/constants.yaml");
    std::fs::remove_file(path)?;

    let mut file = std::fs::File::create(path)?;
    let map: HashMap<String, String> = [
        (
            stringify!(CACHE_POLL_PERIOD).to_ascii_lowercase(),
            constants::CACHE_POLL_PERIOD.to_string(),
        ),
        (
            stringify!(DEFAULT_REQ_TIMEOUT).to_ascii_lowercase(),
            constants::DEFAULT_REQ_TIMEOUT.to_string(),
        ),
    ]
    .iter()
    .cloned()
    .collect();

    write_constants(&mut file, map)?;
    Ok(())
}

fn write_constants(file: &mut std::fs::File, constants: HashMap<String, String>) -> BuildResult {
    file.write_all("base:\n".as_ref())?;
    for (k, v) in constants.into_iter() {
        write_constant(file, "  ", &k, &v)?;
    }
    Ok(())
}

fn write_constant(file: &mut std::fs::File, pad: &str, name: &str, value: &str) -> BuildResult {
    let v = format!("{}{}: \"{}\"\n", pad, name, value);
    file.write_all(v.as_ref())?;
    Ok(())
}
