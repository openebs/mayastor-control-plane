use crate::resources::error::{
    EdgesNotAlphaNumSnafu, KeyIsEmptySnafu, KeyIsNotAlphaNumericPlusSnafu, KeySlashCountSnafu,
    KeyTooLongSnafu, TopologyError, ValueIsEmptySnafu, ValueIsNotAlphaNumericPlusSnafu,
    ValueTooLongSnafu,
};
use prettytable::{format, Row, Table};
use serde::ser;

const CELL_NO_CONTENT: &str = "<none>";

/// Optional cells should display `CELL_NO_CONTENT` if Node
pub fn optional_cell<T: ToString>(field: Option<T>) -> String {
    field
        .map(|f| f.to_string())
        .unwrap_or_else(|| CELL_NO_CONTENT.to_string())
}

// Constants to store the table headers of the Tabular output formats.
lazy_static! {
    pub static ref VOLUME_HEADERS: Row = row![
        "ID",
        "REPLICAS",
        "TARGET-NODE",
        "ACCESSIBILITY",
        "STATUS",
        "SIZE",
        "THIN-PROVISIONED",
        "ALLOCATED",
        "SNAPSHOTS",
        "SOURCE",
    ];
    pub static ref SNAPSHOT_HEADERS: Row = row![
        "ID",
        "TIMESTAMP",
        "SOURCE-SIZE",
        "ALLOCATED-SIZE",
        "TOTAL-ALLOCATED-SIZE",
        "SOURCE-VOL",
        "RESTORES",
        "SNAPSHOT_REPLICAS"
    ];
    pub static ref POOLS_HEADERS: Row = row![
        "ID",
        "DISKS",
        "MANAGED",
        "NODE",
        "STATUS",
        "CAPACITY",
        "ALLOCATED",
        "AVAILABLE",
        "COMMITTED"
    ];
    pub static ref NODE_HEADERS: Row = row!["ID", "GRPC ENDPOINT", "STATUS", "VERSION"];
    pub static ref REPLICA_TOPOLOGIES_PREFIX: Row = row!["VOLUME-ID"];
    pub static ref REPLICA_TOPOLOGY_HEADERS: Row = row![
        "ID",
        "NODE",
        "POOL",
        "STATUS",
        "CAPACITY",
        "ALLOCATED",
        "SNAPSHOTS",
        "CHILD-STATUS",
        "REASON",
        "REBUILD"
    ];
    pub static ref SNAPSHOT_TOPOLOGY_PREFIX: Row = row!["SNAPSHOT-ID"];
    pub static ref SNAPSHOT_TOPOLOGY_HEADERS: Row = row![
        "ID",
        "POOL",
        "SNAPSHOT_STATUS",
        "SIZE",
        "ALLOCATED_SIZE",
        "SOURCE"
    ];
    pub static ref REBUILD_HISTORY_HEADER: Row = row![
        "DST",
        "SRC",
        "STATE",
        "TOTAL",
        "RECOVERED",
        "TRANSFERRED",
        "IS-PARTIAL",
        "START-TIME",
        "END-TIME"
    ];
    pub static ref BLOCKDEVICE_HEADERS_ALL: Row = row![
        "DEVNAME",
        "DEVTYPE",
        "SIZE",
        "AVAILABLE",
        "MODEL",
        "DEVPATH",
        "FSTYPE",
        "FSUUID",
        "MOUNTPOINT",
        "PARTTYPE",
        "MAJOR",
        "MINOR",
        "DEVLINKS",
    ];
    pub static ref BLOCKDEVICE_HEADERS_USABLE: Row = row![
        "DEVNAME",
        "DEVTYPE",
        "SIZE",
        "AVAILABLE",
        "MODEL",
        "DEVPATH",
        "MAJOR",
        "MINOR",
        "DEVLINKS",
    ];
}

/// table_printer takes the above defined headers and the rows created at execution,
/// to create a Tabular output and prints to the stdout.
pub fn table_printer(titles: Row, rows: Vec<Row>) {
    let mut table = Table::new();
    // FORMAT_CLEAN has been set to remove table borders
    table.set_format(*format::consts::FORMAT_CLEAN);
    table.set_titles(titles);
    for row in rows {
        table.add_row(row);
    }
    table.printstd();
}

/// CreateRows trait to be implemented by Vec<`resource`> to create the rows.
pub trait CreateRows {
    fn create_rows(&self) -> Vec<Row>;
}
/// CreateRows trait to be implemented by `resource` to create a row.
pub trait CreateRow {
    fn row(&self) -> Row;
}

/// GetHeaderRow trait to be implemented by Vec<`resource`> to fetch the corresponding headers.
pub trait GetHeaderRow {
    fn get_header_row(&self) -> Row;
}

// OutputFormat to be used as an enum to match the output from args.
#[derive(Debug, Clone, strum_macros::EnumString, strum_macros::AsRefStr)]
#[strum(serialize_all = "lowercase")]
pub enum OutputFormat {
    None,
    Yaml,
    Json,
}
impl OutputFormat {
    /// Check for non output format.
    pub fn none(&self) -> bool {
        matches!(self, Self::None)
    }
}

impl<T> CreateRows for Vec<T>
where
    T: CreateRow,
{
    fn create_rows(&self) -> Vec<Row> {
        self.iter().map(|i| i.row()).collect()
    }
}
impl<T> CreateRows for T
where
    T: CreateRow,
{
    fn create_rows(&self) -> Vec<Row> {
        vec![self.row()]
    }
}

impl<T> GetHeaderRow for Vec<T>
where
    T: GetHeaderRow,
{
    fn get_header_row(&self) -> Row {
        self.get(0)
            .map(GetHeaderRow::get_header_row)
            .unwrap_or_default()
    }
}

pub fn print_table<T>(output: &OutputFormat, obj: T)
where
    T: ser::Serialize,
    T: CreateRows,
    T: GetHeaderRow,
{
    match output {
        OutputFormat::Yaml => {
            // Show the YAML form output if output format is YAML.
            let s = serde_yaml::to_string(&obj).unwrap();
            println!("{s}");
        }
        OutputFormat::Json => {
            // Show the JSON form output if output format is JSON.
            let s = serde_json::to_string(&obj).unwrap();
            println!("{s}");
        }
        OutputFormat::None => {
            // Show the tabular form if output format is not specified.
            let rows: Vec<Row> = obj.create_rows();
            let header: Row = obj.get_header_row();
            table_printer(header, rows);
        }
    }
}

/// Checks if a given character is allowed in topology keys.
///
/// # Description
/// This function determines if a provided character is permissible for use in topology keys.
/// The allowed characters are:
/// - ASCII alphanumeric characters (letters and digits)
/// - Special characters: underscore (`_`), hyphen (`-`), and period (`.`).
///
/// # Parameters
/// - `key`: A `char` representing the character to check.
///
/// # Returns
/// Returns `true` if the character is allowed in topology keys; otherwise, returns `false`.
///
/// # Examples
/// ```
/// assert_eq!(allowed_topology_chars('a'), true); // ASCII letter
/// assert_eq!(allowed_topology_chars('1'), true); // ASCII digit
/// assert_eq!(allowed_topology_chars('_'), true); // Underscore
/// assert_eq!(allowed_topology_chars('-'), true); // Hyphen
/// assert_eq!(allowed_topology_chars('.'), true); // Period
/// assert_eq!(allowed_topology_chars('!'), false); // Not allowed
/// assert_eq!(allowed_topology_chars(' '), false); // Space is not allowed
/// assert_eq!(allowed_topology_chars('@'), false); // Special character not allowed
/// ```
pub fn allowed_topology_chars(key: char) -> bool {
    key.is_ascii_alphanumeric() || matches!(key, '_' | '-' | '.')
}

/// Determines if the provided label string has allowed topology tips.
///
/// # Description
/// This function checks whether the first and last characters of a given string (`label`) are valid
/// for use as topology tips. A valid character for the tips of the string is an ASCII alphanumeric
/// character. If the string is empty, it will return `true`.
///
/// Internally, this function uses a helper function to verify the first and last characters
/// by mapping them to their alphanumeric status or defaulting to `true` if the character is `None`.
pub fn allowed_topology_tips(label: &str) -> bool {
    fn allowed_topology_tips_chars(char: Option<char>) -> bool {
        char.map(|c| c.is_ascii_alphanumeric()).unwrap_or(true)
    }

    allowed_topology_tips_chars(label.chars().next())
        && allowed_topology_tips_chars(label.chars().last())
}

/// Validates a topology key based on specific criteria.
///
/// # Description
/// This function validates a given topology key string to ensure it adheres to a set of predefined
/// rules. These rules include checks for the key's length, content, and structure. The key is
/// considered valid if:
/// - It is not empty.
/// - It does not exceed 63 characters in length.
/// - The first and last characters are ASCII alphanumeric.
/// - It contains at most one slash ('/') character.
/// - All characters are ASCII alphanumeric, underscore ('_'), hyphen ('-'), period ('.'), or a
///   slash ('/').
///
/// If any of these conditions are not met, the function returns an appropriate `TopologyError`.
///
/// # Parameters
/// - `key`: A `&str` reference representing the topology key to be validated.
pub fn validate_topology_key(key: &str) -> Result<(), TopologyError> {
    snafu::ensure!(!key.is_empty(), KeyIsEmptySnafu);
    snafu::ensure!(key.len() <= 63, KeyTooLongSnafu);
    snafu::ensure!(allowed_topology_tips(key), EdgesNotAlphaNumSnafu);

    snafu::ensure!(
        key.chars().filter(|c| c == &'/').count() <= 1,
        KeySlashCountSnafu
    );

    snafu::ensure!(
        key.chars().all(|c| allowed_topology_chars(c) || c == '/'),
        KeyIsNotAlphaNumericPlusSnafu
    );

    Ok(())
}

/// Validates a topology value based on specific criteria.
pub fn validate_topology_value(value: &str) -> Result<(), TopologyError> {
    snafu::ensure!(!value.is_empty(), ValueIsEmptySnafu);
    snafu::ensure!(value.len() <= 63, ValueTooLongSnafu);
    snafu::ensure!(allowed_topology_tips(value), EdgesNotAlphaNumSnafu);
    snafu::ensure!(
        value.chars().all(allowed_topology_chars),
        ValueIsNotAlphaNumericPlusSnafu
    );
    Ok(())
}
