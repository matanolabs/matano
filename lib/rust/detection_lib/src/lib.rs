use pyo3::prelude::*;

use anyhow::{Context, Result};

mod avro_index;
mod enrichment;
use enrichment::*;

// TODO: make this work, need to disable exec-tls maybe not use tikv jemalloc
// #[global_allocator]
// static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[pymodule]
fn detection_lib(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<EnrichmentTable>()?;
    m.add_function(wrap_pyfunction!(create_enrichment_tables, m)?)?;

    Ok(())
}
