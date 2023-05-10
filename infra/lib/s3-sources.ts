import * as path from "path";
import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as sns from "aws-cdk-lib/aws-sns";
import { MatanoLogSource } from "./log-source";
import { commonPathPrefix } from "./utils";
import { SqsSubscription } from "aws-cdk-lib/aws-sns-subscriptions";
import { AwsCustomResource, PhysicalResourceId } from "aws-cdk-lib/custom-resources";
import { MatanoS3SnsSqsSubscription } from "./matano-s3-sns-sqs-subscription";

type FilterSource = {
  bucket_name: string;
  update: string;
  access_role_arn?: string;
  key_prefixes: string[];
};

interface MatanoS3SourcesProps {
  logSources: MatanoLogSource[];
  sourcesIngestionTopic: sns.Topic;
  sourcesIngestionQueue: sqs.Queue;
}

/** Configures custom S3 sources (BYOB), primarily notifications. */
export class MatanoS3Sources extends Construct {
  finalCustomSources: FilterSource[];

  constructor(scope: Construct, id: string, props: MatanoS3SourcesProps) {
    super(scope, id);

    this.finalCustomSources = [];

    // Get sources with custom sources and collect them into buckets + associated prefixes
    for (const logSource of props.logSources) {
      let { bucket_name, key_prefix, access_role_arn, update } = logSource?.logSourceConfig?.ingest?.s3_source ?? {};

      // Assume root if no prefix for BYOB
      if (key_prefix == null) {
        key_prefix = "";
      }

      if (bucket_name == null) {
        continue;
      }

      if (!this.finalCustomSources.map((ls) => ls?.bucket_name).includes(bucket_name)) {
        this.finalCustomSources.push({ bucket_name: bucket_name!!, access_role_arn: access_role_arn!!, update: update!!, key_prefixes: [key_prefix!!] });
      } else {
        this.finalCustomSources.find((ls) => ls?.bucket_name === bucket_name)!!.key_prefixes.push(key_prefix);
      }
    }

    for (const finalSource of this.finalCustomSources) {
      if (finalSource.access_role_arn != null) {
        const subscription = new MatanoS3SnsSqsSubscription(this, `${finalSource.bucket_name}SqsSubscription`, {
          s3: {
            ...finalSource,
            access_role_arn: finalSource.access_role_arn!!,
          },
          queue: props.sourcesIngestionQueue,
        });
      } else {
        // must keep for backwards compatibility to BYO log sources configured without a customer managed SNS / Access Role
        const importedBucket = s3.Bucket.fromBucketName(
          this,
          `ImportedSourcesBucket-${finalSource!!.bucket_name!!}`,
          finalSource!!.bucket_name!!
        );
        // Only one prefix filter allowed, so find common.
        const commonPrefix = commonPathPrefix(finalSource.key_prefixes);
        const filters: s3.NotificationKeyFilter[] = commonPrefix === "" ? [] : [{ prefix: commonPrefix }];
  
        importedBucket.addEventNotification(
          s3.EventType.OBJECT_CREATED,
          new s3n.SnsDestination(props.sourcesIngestionTopic),
          ...filters
        );
      }
    }
  }

  grantRead(construct: iam.IGrantable) {
    const finalBucketAndPrefixes = [];
    for (const finalSource of this.finalCustomSources) {
      for (const rawPrefix of finalSource.key_prefixes) {
        // Remove leading and trailing slashes and add wildcard
        const prefix = path.join(rawPrefix.replace(/^\/|\/$/g, ""), "*");
        finalBucketAndPrefixes.push({ bucketName: finalSource.bucket_name, prefix: prefix });
      }
    }
    const resourceArns = finalBucketAndPrefixes.flatMap((bucketAndPrefix) => {
      return [
        `arn:aws:s3:::${bucketAndPrefix.bucketName}`,
        `arn:aws:s3:::${bucketAndPrefix.bucketName}/${bucketAndPrefix.prefix}`,
      ];
    });
    if (resourceArns.length === 0) {
      return;
    }

    construct.grantPrincipal.addToPrincipalPolicy(
      new iam.PolicyStatement({
        actions: ["s3:GetObject*", "s3:GetBucket*", "s3:List*"],
        resources: resourceArns,
      })
    );
    construct.grantPrincipal.addToPrincipalPolicy(
      new iam.PolicyStatement({
        actions: ["kms:Decrypt", "kms:DescribeKey"],
        resources: ["*"],
      })
    );
    // filter and map to accessRoleArns
    const accessRoleArns = this.finalCustomSources
      .filter((ls) => ls.access_role_arn != null)
      .map((ls) => ls.access_role_arn!!);
    // add to policy, if any
    if (accessRoleArns.length > 0) {
      construct.grantPrincipal.addToPrincipalPolicy(
        new iam.PolicyStatement({
          actions: ["sts:AssumeRole"],
          resources: accessRoleArns
        })
      );
    }
  }
}
