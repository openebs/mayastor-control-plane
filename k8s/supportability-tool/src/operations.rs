use structopt::StructOpt;

/// Types of operations supported by plugin
#[derive(StructOpt, Clone, Debug)]
pub(crate) enum Operations {
    /// 'Dump' creates an archive by collecting provided resource(s) information
    Dump(Resource),
}

/// Resources on which operation can be performed
#[derive(StructOpt, Clone, Debug)]
pub(crate) enum Resource {
    /// Collects entire system information
    System,
}
