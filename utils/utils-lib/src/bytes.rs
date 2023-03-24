/// Converts bytes to human-readable values.
/// Based of the human_bytes crate.
pub fn into_human(bytes: u64) -> String {
    const SUFFIX: [&str; 9] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"];
    const UNIT: f64 = 1024.0;

    let size = bytes as f64;

    if size <= 0.0 {
        return "0 B".to_string();
    }
    let base = size.log10() / UNIT.log10();

    let human_size_prefix = format!("{:.1}", UNIT.powf(base - base.floor()));
    let human_size_prefix = human_size_prefix.trim_end_matches(".0");

    let floor = base.floor() as usize;
    match SUFFIX.get(floor) {
        Some(units) => format!("{human_size_prefix}{units}"),
        None => format!("{bytes} B"),
    }
}
