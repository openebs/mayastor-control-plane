use url::Url;

/// Normalize the disks if they have a schema, we dont want to change anything
/// or do any error checking -- the loop will converge to the error state eventually
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
    let stripped = addr.strip_prefix("pcie://")?;

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
