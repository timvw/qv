use chrono::{DateTime, Utc};
use clap::Parser;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    /// Location where the data is located
    pub path: String,

    /// Query to execute
    #[clap(short, long, default_value_t = String::from("select * from tbl"), group = "sql")]
    query: String,

    /// When provided the schema is shown
    #[clap(short, long, group = "sql")]
    pub schema: bool,

    /// Rows to return
    #[clap(short, long, default_value_t = 10)]
    pub limit: usize,

    /// Optional AWS Profile to use
    #[clap(short, long)]
    pub profile: Option<String>,

    /// Optional timestamp for delta table
    #[clap(
        short,
        long,
        help = "Timestamp to load deltatable in RFC format, eg: 2022-01-13T16:39:00+01:00"
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

    /*
    pub async fn get_globbing_path(&self) -> Result<GlobbingPath> {
        let (data_location, maybe_sdk_config) = match update_s3_console_url(&self.path) {
            (true, updated_location) => (updated_location, Some(get_sdk_config(self).await)),
            (false, location) => (location, None),
        };

        let data_location = match parse_glue_url(&data_location) {
            // When the provided s looks like glue://database.table we lookup the storage location
            // When the provided s does not look like glue://database.table, return s as is.
            Some((database_name, table_name)) => {
                let sdk_config = match maybe_sdk_config {
                    Some(sdk_config) => sdk_config,
                    None => get_sdk_config(self).await,
                };

                get_storage_location(&sdk_config, &database_name, &table_name)
                    .await
                    .unwrap_or_else(|_| {
                        panic!(
                            "failed to get storage location for {}.{}",
                            database_name, table_name
                        )
                    })
            }
            None => data_location,
        };

        let globbing_path = GlobbingPath::parse(&data_location)?;
        Ok(globbing_path)
    }*/
}
