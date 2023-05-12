import * as path from "path";
import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as sns from "aws-cdk-lib/aws-sns";
import { MatanoLogSource } from "../log-source";
import { commonPathPrefix } from "../utils";
import { SqsSubscription } from "aws-cdk-lib/aws-sns-subscriptions";
import { Code, Runtime, SingletonFunction } from "aws-cdk-lib/aws-lambda";
import { readFileSync } from "fs";

const MATANO_SQS_SUBSCRIPTION_SOURCE = `
import boto3, json, logging, urllib.request
from botocore.exceptions import ClientError
import hashlib

iam = boto3.client("iam")

def handler(event, context):
    response_status = "SUCCESS"
    error_message = ""
    topic_arn = ""
    physical_resource_id = ""
    try:
        bucket_name = event["ResourceProperties"]["BucketName"]
        queue_arn = event["ResourceProperties"]["QueueArn"]
        role_arn = event["ResourceProperties"]["AssumeRoleArn"]
        # queue_url = event["ResourceProperties"]["QueueUrl"]
        request_type = event["RequestType"]

        # assume the role
        sts = boto3.client("sts")
        assumed_role = sts.assume_role(RoleArn=role_arn, RoleSessionName="matano")
        credentials = assumed_role["Credentials"]
        # generate s3 client with assumed role credentials
        s3 = boto3.client(
            "s3",
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
        )

        physical_resource_id = hashlib.sha256(f"{bucket_name}{queue_arn}".encode("utf-8")).hexdigest()[:25]

        # lookup sns topic_arn from bucket notification configuration
        notif_conf = None
        try:
            notif_conf = s3.get_bucket_notification_configuration(Bucket=bucket_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "AccessDenied":
                raise Exception(
                    "Matano does not have permission to read the bucket notification configuration. Please ensure that the access role given to Matano has the s3:GetBucketNotificationConfiguration permission"
                )
            else:
                raise e

        # topic arn is the first configuration in reverse order that has anything that starts with s3:ObjectCreated
        try:
            topic_arn = next(
                t for t in reversed(notif_conf["TopicConfigurations"]) if any(e.startswith("s3:ObjectCreated") for e in t["Events"])
            )["TopicArn"]
        except StopIteration:
            if request_type != "Delete":
                raise Exception("Matano could not find a topic configuration with s3:ObjectCreated* in the customer BYO bucket notification configuration")
        
        def unsubscribe(topic_arn):
            for s in sns.list_subscriptions_by_topic(TopicArn=topic_arn)["Subscriptions"]:
                if s["Endpoint"] == queue_arn:
                    sns.unsubscribe(SubscriptionArn=s["SubscriptionArn"])
        def subscribe(topic_arn):
            sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn, Attributes={"RawMessageDelivery": "true"})

        if request_type == "Delete":
            if topic_arn:
                # create sns client with region of topic_arn
                sns = boto3.client("sns", region_name=topic_arn.split(":")[3])
                unsubscribe(topic_arn)      
        else:
            # create sns client with region of topic_arn
            sns = boto3.client("sns", region_name=topic_arn.split(":")[3])
            # create sns subscription with raw message delivery
            try:
                subscribe(topic_arn)
            except ClientError as e:
                # InvalidParameterException is thrown if the subscription already exists with different attributes
                # if this happens, we need to delete the existing subscription and create a new one
                if e.response["Error"]["Code"] == "InvalidParameter":
                    print(
                        "Conflict:recreating subscription.."
                    )
                    unsubscribe(topic_arn)
                    subscribe(topic_arn)
                else:
                    raise e

    except Exception as e:
        response_status = "FAILED"
        error_message = f"Failed: {str(e)}. "
    finally:
        submit_response(
            event, context, response_status, error_message, {"TopicArn": topic_arn}, physical_resource_id
        )

def submit_response(event, context, response_status, error_message, data=None, physical_resource_id=None):
    response_body = {
        "Status": response_status,
        "Reason": f"{error_message}See cwlogs: {context.log_stream_name}",
        "PhysicalResourceId": physical_resource_id or event.get("PhysicalResourceId") or event["LogicalResourceId"],
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "NoEcho": False,
    }
    if data:
        response_body["Data"] = data

    response_body = json.dumps(response_body).encode("utf-8")
    print("Response body:" + response_body.decode("utf-8"))
    headers = {"content-type": "", "content-length": str(len(response_body))}
    try:
        req = urllib.request.Request(url=event["ResponseURL"], headers=headers, data=response_body, method="PUT")
        with urllib.request.urlopen(req) as response:
            print(response.read().decode("utf-8"))
        print("Status code: " + response.reason)
    except Exception as e:
        print("send(..) failed exec request.urlopen(..): " + str(e))
`;

interface MatanoS3SnsSqsSubscriptionProps {
  s3: {
    bucket_name: string;
    access_role_arn: string;
    update: string;
  };
  queue: sqs.IQueue;
}

/**
 * Configures SQS subscription for custom Matano S3 sources (BYOB).
 * This requires an access role and the bucket to be alredy setup with notifications via a customer-managed SNS topic on the customer bucket.
 * */
export class MatanoS3SnsSqsSubscription extends Construct {
  role: iam.IRole;
  constructor(scope: Construct, id: string, props: MatanoS3SnsSqsSubscriptionProps) {
    super(scope, id);

    const {
      s3: { bucket_name, access_role_arn },
      queue,
    } = props;

    const bucket = s3.Bucket.fromBucketName(this, "Bucket", bucket_name);
    this.role = iam.Role.fromRoleArn(this, "Role", access_role_arn);

    // const matanoSqsSubscriptionSource = readFileSync(path.join(__dirname, "lambda/index.py"), "utf8");
    const matanoSqsSubscriptionSourceWithoutComments =
      MATANO_SQS_SUBSCRIPTION_SOURCE.replace(/^ *#.*\n?/gm, "").replace(/(\s|\n)+$/, "");

    if (matanoSqsSubscriptionSourceWithoutComments.length > 4482) {
      throw new Error(
        `Source of Matano SQS Subscription Resource Handler is too large (${matanoSqsSubscriptionSourceWithoutComments.length} > 4096)`
      );
    }

    const matanoSqsSubscriptionHandler = new SingletonFunction(this, "Handler", {
      uuid: "12f3a4b5-6c78-90d1-2e3f-4a5b6c7d8e90",
      runtime: Runtime.PYTHON_3_9,
      code: Code.fromInline(matanoSqsSubscriptionSourceWithoutComments),
      handler: "index.handler",
      timeout: cdk.Duration.seconds(300),
    });

    const matanoSqsSubscription = new cdk.CustomResource(this, "Default", {
      serviceToken: matanoSqsSubscriptionHandler.functionArn,
      properties: {
        ForceUpdate: props.s3.update,
        BucketName: bucket.bucketName,
        QueueArn: queue.queueArn,
        QueueUrl: queue.queueUrl,
        AssumeRoleArn: this.role.roleArn,
      },
    });
    // allow lambda role to assume access role
    this.role.grantAssumeRole(matanoSqsSubscriptionHandler.grantPrincipal);
    // allow matanoSqsSubscriptionHandler.role to subscribe to topics
    matanoSqsSubscriptionHandler.addToRolePolicy(
      new iam.PolicyStatement({
        resources: ["*"],
        actions: ["sns:Subscribe", "sns:Unsubscribe", "sns:ListSubscriptionsByTopic"],
      })
    );

    const topicArn = matanoSqsSubscription.getAttString("TopicArn");
    // make queue allow topic to send messages to it
    queue.addToResourcePolicy(
      new iam.PolicyStatement({
        resources: [queue.queueArn],
        actions: ["sqs:SendMessage"],
        principals: [new iam.ServicePrincipal("sns.amazonaws.com")],
        conditions: {
          ArnEquals: { "aws:SourceArn": topicArn },
        },
      })
    );

    // if queue has encryption key, make it allow topic to decrypt
    if (queue.encryptionMasterKey) {
      queue.encryptionMasterKey.addToResourcePolicy(
        new iam.PolicyStatement({
          resources: ["*"],
          actions: ["kms:Decrypt", "kms:GenerateDataKey"],
          principals: [new iam.ServicePrincipal("sns.amazonaws.com")],
          conditions: { ArnEquals: { "aws:SourceArn": topicArn } },
        })
      );
    }
  }
}
