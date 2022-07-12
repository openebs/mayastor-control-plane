use crate::collect::resources::{
    traits::{ResourceInformation, TablePrinter, Topologer},
    ResourceError,
};
use lazy_static::lazy_static;
use prettytable::{format, Row, Table};
use serde::{ser, Serialize};
use std::{collections::HashSet, io};

/// Defines maximum entries REST service can fetch at one network call
pub(crate) const MAX_RESOURCE_ENTRIES: isize = 200;

// Constants to store the table headers of the Tabular output formats.
lazy_static! {
    /// Represents list of Volume table headers
    pub(crate) static ref VOLUME_HEADERS: Row = row!["INDEX", "ID", "STATUS", "SIZE",];
    /// Represents list of pool table headers
    pub(crate) static ref POOL_HEADERS: Row = row!["INDEX", "ID", "DISKS", "NODE", "STATUS",];
    /// Represents list of node table headers
    pub(crate) static ref NODE_HEADERS: Row = row!["INDEX", "ID", "STATUS"];
}

/// Prints list of resources in tabular format and read input based on index
pub(crate) fn print_table_and_get_id<T>(obj: T) -> Result<String, ResourceError>
where
    T: TablePrinter,
    T: ser::Serialize,
{
    let rows: Vec<Row> = obj.create_rows();
    let header: Row = obj.get_header_row();
    let mut table = Table::new();

    table.set_format(*format::consts::FORMAT_CLEAN);
    table.set_titles(header);
    for row in rows {
        table.add_row(row);
    }
    table.printstd();

    println!("Please enter index of resource: ");
    let mut index: usize;
    let mut input_line = String::new();
    loop {
        input_line.clear();
        let _no_of_chars = io::stdin().read_line(&mut input_line)?;
        let trimmed_input = input_line.trim().trim_end_matches('\n');
        if trimmed_input.is_empty() {
            continue;
        }
        index = trimmed_input.parse::<usize>().unwrap();
        if index > table.len() {
            println!("Please enter number in range of (1, {})", table.len());
            continue;
        }
        break;
    }
    index -= 1;
    let row_data = table.get_row(index).ok_or_else(|| {
        ResourceError::CustomError("Unable to get resource information".to_string())
    })?;
    obj.get_resource_id(row_data)
}

impl<T> TablePrinter for Vec<T>
where
    T: TablePrinter,
{
    fn create_rows(&self) -> Vec<Row> {
        self.iter()
            .enumerate()
            .flat_map(|(index, r)| -> Vec<Row> {
                let mut row_data: Row = r.create_rows()[0].clone();
                row_data.insert_cell(0, cell![index + 1]);
                vec![row_data]
            })
            .collect()
    }

    fn get_header_row(&self) -> Row {
        self.get(0).map(|obj| obj.get_header_row()).unwrap()
    }

    fn get_resource_id(&self, row_data: &Row) -> Result<String, ResourceError> {
        self.get(0).unwrap().get_resource_id(row_data)
    }
}

impl<T> Topologer for Vec<T>
where
    T: Topologer + Serialize,
{
    fn get_printable_topology(&self) -> Result<(String, String), ResourceError> {
        let topology_as_pretty = serde_json::to_string_pretty(self)?;
        Ok(("all-topology.json".to_string(), topology_as_pretty))
    }

    fn dump_topology_info(&self, dir_path: String) -> Result<(), ResourceError> {
        for obj in self.iter() {
            obj.dump_topology_info(dir_path.clone())?;
        }
        Ok(())
    }

    fn get_unhealthy_resource_info(&self) -> HashSet<ResourceInformation> {
        let mut resources = HashSet::new();
        for topo in self.iter() {
            resources.extend(topo.get_unhealthy_resource_info());
        }
        resources
    }

    fn get_all_resource_info(&self) -> HashSet<ResourceInformation> {
        let mut resources = HashSet::new();
        for topo in self.iter() {
            resources.extend(topo.get_all_resource_info());
        }
        resources
    }

    fn get_k8s_resource_names(&self) -> Vec<String> {
        self.iter()
            .flat_map(|t| t.get_k8s_resource_names())
            .collect::<Vec<String>>()
    }
}
