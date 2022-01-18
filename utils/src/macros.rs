/// Print package related information. This includes the package name, version and git hash.
#[macro_export]
macro_rules! print_package_info {
    () => {
        println!(
            "{} version {}",
            env!("CARGO_PKG_NAME"),
            env!("CARGO_PKG_VERSION"),
        )
    };

    ($git_version:expr) => {
        println!(
            "{} version {}, git hash {}",
            env!("CARGO_PKG_NAME"),
            env!("CARGO_PKG_VERSION"),
            $git_version,
        )
    };
}
