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
        "SIZE"
    ];
    pub static ref POOLS_HEADERS: Row = row![
        "ID",
        "TOTAL CAPACITY",
        "USED CAPACITY",
        "DISKS",
        "NODE",
        "STATUS",
        "MANAGED"
    ];
    pub static ref NODE_HEADERS: Row = row!["ID", "GRPC ENDPOINT", "STATUS", "CORDONED"];
    pub static ref REPLICA_TOPOLOGY_HEADERS: Row = row!["ID", "NODE", "POOL", "STATUS"];
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

// table_printer takes the above defined headers and the rows created at execution,
// to create a Tabular output and prints to the stdout.
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

// CreateRows trait to be implemented by Vec<`resource`> to create the rows.
pub trait CreateRows {
    fn create_rows(&self) -> Vec<Row>;
}

// GetHeaderRow trait to be implemented by Vec<`resource`> to fetch the corresponding headers.
pub trait GetHeaderRow {
    fn get_header_row(&self) -> Row;
}

// OutputFormat to be used as an enum to match the output from args.
#[derive(Debug, strum_macros::EnumString, strum_macros::AsRefStr)]
#[strum(serialize_all = "lowercase")]
pub enum OutputFormat {
    None,
    Yaml,
    Json,
}

impl<T> CreateRows for Vec<T>
where
    T: CreateRows,
{
    fn create_rows(&self) -> Vec<Row> {
        self.iter().flat_map(|i| i.create_rows()).collect()
    }
}

// GetHeaderRow trait to be implemented by Volume/Pool to fetch the corresponding headers.
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
            println!("{}", s);
        }
        OutputFormat::Json => {
            // Show the JSON form output if output format is JSON.
            let s = serde_json::to_string(&obj).unwrap();
            println!("{}", s);
        }
        OutputFormat::None => {
            // Show the tabular form if output format is not specified.
            let rows: Vec<Row> = obj.create_rows();
            let header: Row = obj.get_header_row();
            table_printer(header, rows);
        }
    }
}
