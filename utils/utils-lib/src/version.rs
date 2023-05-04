pub mod macros {
    /// Makes a version info instance.
    #[macro_export]
    macro_rules! version_info {
        () => {{
            $crate::version_info_inner!(None, $crate::long_raw_version_str())
        }};
    }

    /// Returns a version info instance.
    #[macro_export]
    macro_rules! package_description {
        () => {
            $crate::version_info!().fmt_description()
        };
    }

    /// Gets package's version info as a String.
    #[macro_export]
    macro_rules! version_info_string {
        () => {
            String::from($crate::version_info!())
        };
    }

    /// Gets package's version info as a static str.
    /// Each call to this macro leaks a string.
    #[macro_export]
    macro_rules! version_info_str {
        () => {
            Box::leak(Box::new(String::from($crate::version_info!()))) as &'static str
        };
    }

    /// Formats package related information.
    /// This includes the package name and version, and commit info.
    #[macro_export]
    macro_rules! fmt_package_info {
        () => {{
            let vi = $crate::version_info!();
            format!("{} {}", vi.fmt_description(), vi)
        }};
    }

    /// Prints package related information.
    /// This includes the package name and version, and commit info.
    #[macro_export]
    macro_rules! print_package_info {
        () => {
            println!("{}", $crate::fmt_package_info!());
        };
    }
}

/// Analogous to `version_info::raw_version_string`.
pub fn raw_version_string() -> String {
    String::from(raw_version_str())
}

/// Analogous to `version_info::raw_version_str`.
pub fn raw_version_str() -> &'static str {
    // to keep clippy happy
    #[allow(dead_code)]
    fn fallback() -> &'static str {
        option_env!("GIT_VERSION").expect("git version fallback")
    }

    git_version_macro::git_version!(
        args = ["--abbrev=12", "--always"],
        fallback = fallback(),
        git_deps = []
    )
}

/// Analogous to `version_info::long_raw_version_str`.
pub fn long_raw_version_str() -> &'static str {
    // to keep clippy happy
    #[allow(dead_code)]
    fn fallback() -> &'static str {
        option_env!("GIT_VERSION_LONG").expect("git version fallback")
    }

    git_version_macro::git_version!(
        args = ["--abbrev=12", "--always", "--long"],
        fallback = fallback(),
        git_deps = []
    )
}
