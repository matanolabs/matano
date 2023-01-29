use lru::LruCache;
use std::{cell::RefCell, collections::BTreeMap, time::Instant};

use vrl::{diagnostic::Formatter, state, Program, Runtime, TargetValueRef};
use vrl::{Terminate, TimeZone};

use anyhow::{anyhow, Context, Result};
use log::{debug, error, info, warn};

thread_local! {
    pub static RUNTIME: RefCell<Runtime> = RefCell::new(Runtime::new(state::Runtime::default()));
}

/// Returns None if program was aborted.
pub fn vrl_opt<'a>(
    program: &'a str,
    value: &'a mut ::value::Value,
) -> Result<Option<(::value::Value, &'a mut ::value::Value)>> {
    thread_local!(
        static CACHE: RefCell<LruCache<String, Result<Program, String>>> =
            RefCell::new(LruCache::new(std::num::NonZeroUsize::new(400).unwrap()));
    );

    CACHE.with(|c| {
        let mut cache_ref = c.borrow_mut();
        let stored_result = (*cache_ref).get(program);

        let start = Instant::now();
        let compiled = match stored_result {
            Some(compiled) => match compiled {
                Ok(compiled) => Ok(compiled),
                Err(e) => {
                    return Err(anyhow!(e.clone()));
                }
            },
            None => match vrl::compile(&program, &vrl_stdlib::all()) {
                Ok(result) => {
                    debug!(
                        "Compiled a vrl program ({}), took {:?}",
                        program
                            .lines()
                            .into_iter()
                            .skip(1)
                            .next()
                            .unwrap_or("expansion"),
                        start.elapsed()
                    );
                    (*cache_ref).put(program.to_string(), Ok(result.program));
                    if result.warnings.len() > 0 {
                        warn!("{:?}", result.warnings);
                    }
                    match (*cache_ref).get(program) {
                        Some(compiled) => match compiled {
                            Ok(compiled) => Ok(compiled),
                            Err(e) => {
                                return Err(anyhow!(e.clone()));
                            }
                        },
                        None => unreachable!(),
                    }
                }
                Err(diagnostics) => {
                    let msg = Formatter::new(&program, diagnostics).to_string();
                    (*cache_ref).put(program.to_string(), Err(msg.clone()));
                    Err(anyhow!(msg))
                }
            },
        }?;

        let mut metadata = ::value::Value::Object(BTreeMap::new());
        let mut secrets = ::value::Secrets::new();
        let mut target = TargetValueRef {
            value: value,
            metadata: &mut metadata,
            secrets: &mut secrets,
        };

        let time_zone_str = Some("tt".to_string()).unwrap_or_default();

        let time_zone = match TimeZone::parse(&time_zone_str) {
            Some(tz) => tz,
            None => TimeZone::Local,
        };

        let result = RUNTIME.with(|r| {
            let mut runtime = r.borrow_mut();

            match (*runtime).resolve(&mut target, &compiled, &time_zone) {
                Ok(result) => Ok(Some(result)),
                Err(Terminate::Abort(_)) => Ok(None),
                Err(e) => Err(anyhow!(e)),
            }
        })?;

        Ok(result.and_then(|output| Some((output, value))))
    })
}

/// Fails if program aborted.
pub fn vrl<'a>(
    program: &'a str,
    value: &'a mut ::value::Value,
) -> Result<(::value::Value, &'a mut ::value::Value)> {
    vrl_opt(program, value)?.context("vrl program aborted")
}
