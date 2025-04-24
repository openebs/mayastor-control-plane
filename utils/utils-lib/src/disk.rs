use url::Url;

/// Normalize the disks if they have a schema, we don't want to change anything
/// or do any error checking.
pub fn normalize_disk(disk: &str) -> String {
    let disk = if disk.starts_with("pcie://") {
        canonical_pcie_address(disk).unwrap_or_else(|| disk.to_string())
    } else {
        disk.to_string()
    };

    Url::parse(&disk).map_or(disk.to_string(), |u| {
        u.to_file_path()
            .unwrap_or_else(|_| disk.into())
            .as_path()
            .display()
            .to_string()
    })
}

fn canonical_pcie_address(addr: &str) -> Option<String> {
    let stripped = addr.strip_prefix("pcie:///")?;

    let parts: Vec<&str> = stripped.split([':', '.']).collect();
    if parts.len() != 4 {
        return None;
    }

    let domain = format!("{:04x}", u16::from_str_radix(parts[0], 16).ok()?);
    let bus = format!("{:02x}", u8::from_str_radix(parts[1], 16).ok()?);
    let device = format!("{:02x}", u8::from_str_radix(parts[2], 16).ok()?);
    let function = format!("{}", parts[3].parse::<u8>().ok()?);

    Some(format!("pcie:///{domain}:{bus}:{device}.{function}"))
}

#[cfg(test)]
mod tests {
    use crate::disk::{canonical_pcie_address, normalize_disk};
    use regex::Regex;

    #[test]
    fn persistent_devlink_regex() {
        fn is_persistent_devlink(pattern: &str) -> bool {
            let re = Regex::new(crate::DEVLINK_REGEX).expect("DEVLINK_REGEX should be valid");
            re.is_match(pattern)
        }

        let test_suite = [
            ("/dev/some/path", false),
            ("/dev/mapper/some/path", false),
            ("/dev/disk/some/path", false),
            ("/dev/disk/by-something/path", false),
            ("/devil/disk/by-something/path", false),
            ("/dev/diskxyz/by-something/path", false),
            ("/dev/disk/something-by/path", false),
            ("/dev/disk/by-/path", false),
            ("something", false),
            ("/dev/somevg/lv0", false),
            ("/dev/disk/by-id/somepath", true),
            ("/dev/disk/by-path/somepath", true),
            ("/dev/disk/by-label/somepath", true),
            ("/dev/disk/by-partuuid/somepath", true),
            ("/dev/disk/by-partlabel/somepath", true),
            ("/dev/mapper/dm0", true),
        ];

        for test in test_suite {
            assert_eq!(is_persistent_devlink(test.0), test.1);
        }
    }

    #[test]
    fn normalize_disk_test() {
        let test_suite = [
            ("aio:///dev/null", "/dev/null"),
            ("uring:///dev/null", "/dev/null"),
            ("uring://dev/null", "uring://dev/null"),
            ("pcie:///0000:01:00.0", "/0000:01:00.0"),
            ("pcie:///00:01:00.0", "/0000:01:00.0"),
            ("pcie:///12454:01:00.0", "/12454:01:00.0"),
        ];
        for test in test_suite {
            assert_eq!(normalize_disk(test.0), test.1);
        }
    }

    #[test]
    fn canonical_pcie_address_test() {
        let test_suite = [
            (
                "pcie:///0000:01:00.0",
                Some("pcie:///0000:01:00.0".to_string()),
            ),
            (
                "pcie:///00:01:00.0",
                Some("pcie:///0000:01:00.0".to_string()),
            ),
            (
                "pcie:///12454:01:00.0", // Invalid address
                None,
            ),
            (
                "pcie:///0011:0111:00.0", // Invalid address
                None,
            ),
        ];
        for test in test_suite {
            assert_eq!(canonical_pcie_address(test.0), test.1);
        }
    }
}
