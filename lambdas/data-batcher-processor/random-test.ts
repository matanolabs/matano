/// <reference types="aws-sdk" />

import { Context, S3Event, S3EventRecord, SQSEvent } from "aws-lambda";
import * as AWS from "aws-sdk";

export async function handler(event: SQSEvent, context: Context) {
    console.log("START");
}
