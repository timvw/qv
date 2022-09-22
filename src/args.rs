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
    schema: bool,

    /// Rows to return
    #[clap(short, long, default_value_t = 10)]
    pub limit: usize,

    /// Optional AWS Profile to use
    #[clap(short, long)]
    pub profile: Option<String>,
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
