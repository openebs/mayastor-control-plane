/// Print package related information.
/// This includes the package name and version. The git hash will also be included if supplied.
#[macro_export]
macro_rules! print_package_info {
    () => {
        println!(
            "{} version {}, git hash {}",
            env!("CARGO_PKG_NAME"),
            env!("CARGO_PKG_VERSION"),
            utils::git_version()
        )
    };
}

/// Get package version and git hash.
#[macro_export]
macro_rules! package_info {
    () => {
        Box::leak(Box::new(format!(
            "version {}, git hash {}",
            env!("CARGO_PKG_VERSION"),
            utils::git_version(),
        ))) as &'static str
    };
}
