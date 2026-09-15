mod datafusion;
mod polars;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    // Run Polars
    polars::run_all_polars_queries()?;

    // Run datafusion
    let runtime = tokio::runtime::Runtime::new()?;

    runtime.block_on(async { datafusion::run_all_datafusion_queries().await })?;

    Ok(())
}
