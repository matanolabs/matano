// This example requires the following input to succeed:
// { "command": "do something" }
mod models;
mod utils;

use models::*;
use shared::*;

use std::env::var;
use std::time::Instant;

use lambda_runtime::{handler_fn, Context, Error};
use log::{debug, error, info};

type Response = Result<SuccessResponse, FailureResponse>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    setup_logging();

    let start = Instant::now();
    let func = handler_fn(my_handler);
    lambda_runtime::run(func).await?;
    debug!("Call lambda took {:.2?}", start.elapsed());

    Ok(())
}

pub(crate) async fn my_handler(event: Request, ctx: Context) -> Response {
    info!("Request: {:?}", event);

    // optional feature flags as defined in `Cargo.toml`. this is enabled
    // in the CDK code in the `lib/` folder.
    utils::log_enabled_features();

    #[cfg(feature = "my-prod-feature")]
    {
        debug!("Now running in a PROD environment (my-prod-feature). Beep... Boop.");
        let answer = systems::system_a::do_stuff_from_system_a();
        info!(
            "The answer to life, the universe, and everything is: {}",
            answer
        );
    }

    let bucket_name = var("BUCKET_NAME")
        .expect("A BUCKET_NAME must be set in this app's Lambda environment variables.");

    // extract some useful info from the request
    let command = event.command;

    // Generate a filename based on when the request was received.
    let filename = format!("{}.txt", time::OffsetDateTime::now_utc().unix_timestamp());

    let start = Instant::now();

    let config = aws_config::load_from_env().await;
    let s3 = aws_sdk_s3::Client::new(&config);

    debug!("Created an S3 client in {:.2?}", start.elapsed());

    let _ = s3
        .put_object()
        .bucket(bucket_name)
        .body(command.as_bytes().to_owned().into())
        .key(&filename)
        .content_type("text/plain")
        .send()
        .await
        .map_err(|err| {
            // In case of failure, log a detailed error to CloudWatch.
            error!(
                "failed to upload file '{}' to S3 with error: {}",
                &filename, err
            );
            // The sender of the request receives this message in response.
            FailureResponse {
                body: "The lambda encountered an error and your message was not saved".to_owned(),
            }
        })?;

    info!(
        "Successfully stored the incoming request in S3 with the name '{}'",
        &filename
    );

    // prepare the response
    let resp = SuccessResponse {
        req_id: ctx.request_id,
        msg: format!("Hello from Lambda 1! The command {} executed.", command),
    };

    // return `Response` (it will be serialized to JSON automatically by the runtime)
    Ok(resp)
}
