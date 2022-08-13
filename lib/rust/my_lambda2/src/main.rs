// This example requires the following input to succeed:
// { "command": "do something" }
mod models;

use models::*;
use shared::systems::*;
use shared::*;

use std::time::Instant;

use anyhow::{Context, Result};
use lambda_runtime::{handler_fn, Context as LambdaContext, Error};
use log::{debug, info};

type Response = Result<SuccessResponse>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    setup_logging();

    system_a::do_stuff_from_system_a();
    system_b::do_stuff_from_system_b();

    let start = Instant::now();
    let func = handler_fn(my_handler);
    lambda_runtime::run(func).await?;
    debug!("Call lambda took {:.2?}", start.elapsed());

    Ok(())
}

pub(crate) async fn my_handler(event: Request, ctx: LambdaContext) -> Response {
    info!("Request: {:?}", event);

    // retrieve an environment variable set at build (compile) time.
    let rust_lang_url = env!("LEARN_RUST_URL");

    // extract some useful info from the request
    let command = event.command;

    let start = Instant::now();

    // This will POST a body of `foo=bar&baz=quux`
    let params = [("foo", "bar"), ("baz", "quux")];
    let client = reqwest::Client::new();
    let res = client
        .post("https://httpbin.org/post")
        .form(&params)
        .send()
        .await
        .context("Failed to send POST request to httpbin")?;

    debug!("Made POST request in {:.2?}", start.elapsed());

    info!(
        "Status: {}, Response: {}",
        res.status(),
        res.text()
            .await
            .context("Failed to get the POST text from first response")?
    );

    let start = Instant::now();

    debug!("Fetching {:?}...", rust_lang_url);

    // reqwest::get() is a convenience function.
    //
    // In most cases, you should create/build a reqwest::Client and reuse
    // it for all requests.
    let res = reqwest::get(rust_lang_url)
        .await
        .context(format!("Failed to GET {}", rust_lang_url))?;

    debug!("Made GET request in {:.2?}", start.elapsed());

    info!("Response: {:?} {}", res.version(), res.status());
    info!("Headers: {:#?}\n", res.headers());

    let body = res.text().await?;

    println!("{}", body);

    let start = Instant::now();

    let p = Person {
        first_name: "Foo".into(),
        last_name: "Bar".into(),
    };

    let res = reqwest::Client::new()
        .post("https://httpbin.org/anything")
        .json(&p)
        .send()
        .await
        .context("Failed to POST the second httpbin request")?;

    let js = res
        .json::<PersonResponse>()
        .await
        .context("Failed to read JSON")?;

    debug!("Made POST request in {:.2?}", start.elapsed());

    let person: Person =
        serde_json::from_str(&js.data).context("Failed to deserialize JSON to a Person object")?;
    println!("{:#?}", person);

    println!("Headers: {:#?}", js.headers);

    // prepare the response
    let resp = SuccessResponse {
        req_id: ctx.request_id,
        msg: format!("Hello from Lambda 2! The command {} executed.", command),
    };

    // return `Response` (it will be serialized to JSON automatically by the runtime)
    Ok(resp)
}
