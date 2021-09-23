use prettytable::{format, Row, Table};
use serde::ser;

// Constant to specify the output formats, these should work irrespective of case.
pub const YAML_FORMAT: &str = "yaml";
pub const JSON_FORMAT: &str = "json";

// Constants to store the table headers of the Tabular output formats.
lazy_static! {
    pub static ref VOLUME_HEADERS: Row = row!["ID", "REPLICAS", "PROTOCOL", "STATUS", "SIZE"];
    pub static ref POOLS_HEADERS: Row = row![
        "ID",
        "TOTAL CAPACITY",
        "USED CAPACITY",
        "DISKS",
        "NODE",
        "STATUS",
        "MANAGED"
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

// CreateRows trait to be implemented by Vec<Volume/Pool> to create the rows.
pub trait CreateRows {
    fn create_rows(&self) -> Vec<Row>;
}

// GetHeaderRow trait to be implemented by Volume/Pool to fetch the corresponding headers.
pub trait GetHeaderRow {
    fn get_header_row(&self) -> Row;
}

// OutputFormat to be used as an enum to match the output from args.
#[derive(Debug)]
pub enum OutputFormat {
    Yaml,
    Json,
    NoFormat,
}

impl From<&str> for OutputFormat {
    fn from(format_str: &str) -> Self {
        match format_str {
            YAML_FORMAT => Self::Yaml,
            JSON_FORMAT => Self::Json,
            _ => Self::NoFormat,
        }
    }
}

impl<T> CreateRows for Vec<T>
where
    T: CreateRows,
{
    fn create_rows(&self) -> Vec<Row> {
        let mut rows = vec![];
        self.iter().for_each(|i| rows.extend(i.create_rows()));
        rows
    }
}

// GetHeaderRow trait to be implemented by Volume/Pool to fetch the corresponding headers.
impl<T> GetHeaderRow for Vec<T>
where
    T: GetHeaderRow,
{
    fn get_header_row(&self) -> Row {
        self[0].get_header_row()
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
        OutputFormat::NoFormat => {
            // Show the tabular form if output format is not specified.
            let rows: Vec<Row> = obj.create_rows();
            let header: Row = obj.get_header_row();
            table_printer(header, rows);
        }
    }
}
