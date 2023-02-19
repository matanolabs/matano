import * as path from "path";
import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as sns from "aws-cdk-lib/aws-sns";
import { MatanoLogSource } from "./log-source";
import { commonPathPrefix } from "./utils";

type FilterSource = {
  bucket_name: string;
  key_prefixes: string[];
};

interface MatanoS3SourcesProps {
  logSources: MatanoLogSource[];
  sourcesIngestionTopic: sns.Topic;
}

/** Configures custom S3 sources (BYOB), primarily notifications. */
export class MatanoS3Sources extends Construct {
  finalCustomSources: FilterSource[];
  constructor(scope: Construct, id: string, props: MatanoS3SourcesProps) {
    super(scope, id);

    this.finalCustomSources = [];

    // Get sources with custom sources and collect them into buckets + associated prefixes
    for (const logSource of props.logSources) {
      let { bucket_name, key_prefix } = logSource?.logSourceConfig?.ingest?.s3_source ?? {};

      // Assume root if no prefix for BYOB
      if (key_prefix == null) {
        key_prefix = "";
      }

      if (bucket_name == null) {
        continue;
      }

      if (!this.finalCustomSources.map((ls) => ls?.bucket_name).includes(bucket_name)) {
        this.finalCustomSources.push({ bucket_name: bucket_name!!, key_prefixes: [key_prefix!!] });
      } else {
        this.finalCustomSources.find((ls) => ls?.bucket_name === bucket_name)!!.key_prefixes.push(key_prefix);
      }
    }

    for (const finalSource of this.finalCustomSources) {
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
    construct.grantPrincipal.addToPrincipalPolicy(
      new iam.PolicyStatement({
        actions: ["s3:GetObject*", "s3:GetBucket*", "s3:List*"],
        resources: resourceArns,
      })
    );
  }
}
