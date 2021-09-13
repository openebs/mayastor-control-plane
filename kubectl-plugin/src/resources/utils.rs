use prettytable::{format, Row, Table};
use serde::ser;

// Constant to specify the output formats, these should work irrespective of case.
pub const YAML_FORMAT: &str = "yaml";
pub const JSON_FORMAT: &str = "json";

// Constants to store the table headers of the Tabular output formats.
lazy_static! {
    pub static ref VOLUME_HEADERS: Row =
        row!["ID", "PATHS", "REPLICAS", "PROTOCOL", "STATUS", "SIZE"];
    pub static ref POOLS_HEADERS: Row = row![
        "ID",
        "TOTAL CAPACITY",
        "USED CAPACITY",
        "DISKS",
        "NODE",
        "STATUS"
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

// // create_volume_rows takes the marshalled Volume json response from REST and creates
// // rows out of it, for Volume Tabular Output.
// pub fn create_volume_rows(volumes: Vec<Volume>) -> Vec<Row> {
//     let mut rows: Vec<Row> = Vec::new();
//     for volume in volumes {
//         let state = volume.state.unwrap();
//         rows.push(row![
//             state.uuid,
//             volume.spec.num_paths,
//             volume.spec.num_replicas,
//             state.protocol,
//             state.status,
//             state.size
//         ]);
//     }
//     rows
// }
//
// // create_volume_rows takes the marshalled Volume json response from REST and creates
// // rows out of it, for Volume Tabular Output.
// pub fn create_pool_rows(pools: Vec<Pool>) -> Vec<Row> {
//     let mut rows: Vec<Row> = Vec::new();
//     for pool in pools {
//         let state = pool.state.unwrap();
//         // The disks are joined by a comma and shown in the table, ex /dev/vda, /dev/vdb
//         let disks = state.disks.join(", ");
//         rows.push(row![
//             state.id,
//             state.capacity,
//             state.used,
//             disks,
//             state.node,
//             state.status
//         ]);
//     }
//     rows
// }

pub trait CreateRows {
    fn create_rows(&self) -> Vec<Row>;
}

pub enum OutputFormat {
    Yaml,
    Json,
    NoFormat,
}

impl From<&str> for OutputFormat {
    fn from(format_str: &str) -> Self {
        if format_str == YAML_FORMAT {
            Self::Yaml
        } else if format_str == JSON_FORMAT {
            Self::Json
        } else {
            Self::NoFormat
        }
    }
}

pub fn print_table<T>(output: OutputFormat, obj: Vec<T>)
where
    T: ser::Serialize,
    Vec<T>: CreateRows,
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
        _ => {
            // Show the tabular form if output format is not specified.
            let rows: Vec<Row> = obj.create_rows();
            table_printer((&*VOLUME_HEADERS).clone(), rows);
        }
    }
}
