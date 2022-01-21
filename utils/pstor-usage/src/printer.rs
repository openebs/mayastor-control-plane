/// Data points that can be organized into a table.
pub(crate) trait TabledData {
    /// Specify the types used for each Row.
    /// We could simply have Strings here but this we avoid having to convert again.
    type Row;
    /// Title or first row (usually the name/identifier of each column).
    fn titles(&self) -> Self::Row;
    /// The remaining rows.
    fn rows(&self) -> Vec<Self::Row>;
}

/// Something that can print `TabledData` for a specific `Row` type.
pub(crate) trait Printer {
    type Row;
    fn print(&self, printable: &impl TabledData<Row = Self::Row>);
}

/// A `Printer` that prints `TabledData` with prettyprinter Rows.
pub(crate) struct PrettyPrinter {}
impl PrettyPrinter {
    pub(crate) fn new() -> Self {
        Self {}
    }
}
impl Printer for PrettyPrinter {
    type Row = prettytable::Row;

    fn print(&self, printable: &impl TabledData<Row = Self::Row>) {
        let rows = printable.rows();
        if rows.is_empty() {
            // no point printing empty tables
            return;
        }

        let titles = printable.titles();

        let mut table = prettytable::Table::init(rows);
        table.set_format(*prettytable::format::consts::FORMAT_BOX_CHARS);
        table.set_titles(titles);

        table.printstd();
    }
}
