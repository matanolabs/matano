import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as sns from "aws-cdk-lib/aws-sns";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";

interface MatanoAlertingProps {
}

export class MatanoAlerting extends Construct {
  alertingTopic: sns.Topic;
  constructor(scope: Construct, id: string, props: MatanoAlertingProps) {
    super(scope, id);
    
    this.alertingTopic =  new sns.Topic(this, "MatanoAlertingTopic", {
        displayName: "MatanoAlertingTopic",
    });

  }
}
