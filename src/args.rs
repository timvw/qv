use chrono::{DateTime, Utc};
use clap::Parser;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    /// Location where the data is located
    pub path: String,

    /// Query to execute
    #[clap(short, long, default_value_t = String::from("select * from tbl"), group = "sql")]
    pub query: String,

    /// When provided the schema is shown
    #[clap(short, long, group = "sql")]
    pub schema: bool,

    /// Rows to return
    #[clap(short, long, default_value_t = 10)]
    pub limit: usize,

    /// Optional AWS Profile to use
    #[clap(short, long)]
    pub profile: Option<String>,

    /// Iceberg REST catalog URI. When set, PATH is a namespace-qualified table name
    #[clap(long, value_name = "URI")]
    pub rest_catalog: Option<String>,

    /// Optional warehouse passed to the Iceberg REST catalog
    #[clap(long, value_name = "LOCATION", requires = "rest_catalog")]
    pub catalog_warehouse: Option<String>,

    /// Iceberg REST catalog or storage property (KEY=VALUE); may be repeated
    #[clap(long = "catalog-property", value_name = "KEY=VALUE")]
    pub catalog_properties: Vec<String>,

    /// Iceberg property sourced from an environment variable (KEY=ENV_VAR); may be repeated
    #[clap(long = "catalog-property-env", value_name = "KEY=ENV_VAR")]
    pub catalog_property_env: Vec<String>,

    /// Optional timestamp for Delta Lake or Iceberg time travel
    #[clap(
        short,
        long,
        help = "Timestamp for Delta Lake or Iceberg time travel in RFC format, eg: 2022-01-13T16:39:00+01:00"
    )]
    pub at: Option<DateTime<Utc>>,
}

impl Args {
    pub fn get_query(&self) -> &str {
        let query = if self.schema {
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'tbl'"
        } else {
            self.query.as_str()
        };
        query
    }
}
