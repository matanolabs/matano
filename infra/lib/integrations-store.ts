import { Construct } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as ddb from "aws-cdk-lib/aws-dynamodb";
import * as kms from "aws-cdk-lib/aws-kms";
import * as secrets from "aws-cdk-lib/aws-secretsmanager";
import * as path from "path";

import { MatanoStack } from "./MatanoStack";
import { getDirectories, matanoResourceToCdkName, readConfigPath, walkdirSync } from "./utils";
import { SecretValue } from "aws-cdk-lib";

interface IntegrationsStoreProps {
  customerManagedKmsKey?: kms.IKey;
}
export class IntegrationsStore extends Construct {
  integrationsInfoMap: Record<string, Record<string, any> | undefined>;
  integrationsSecretMap: Record<string, secrets.Secret>;
  sharedSecret: secrets.Secret;

  constructor(scope: Construct, id: string, props: IntegrationsStoreProps) {
    super(scope, id);
    const matanoUserDirectory = (cdk.Stack.of(this) as MatanoStack).matanoUserDirectory;

    this.integrationsSecretMap = {};
    this.integrationsInfoMap = {};

    const destinationsDirectory = path.join(matanoUserDirectory, "integrations/destinations");
    const destinationConfigPaths = walkdirSync(destinationsDirectory);

    for (const destinationConfigPath of destinationConfigPaths) {
      let destinationConfig = readConfigPath(destinationConfigPath);

      validateDestinationConfig(destinationConfig);

      destinationConfig.type = destinationConfig.type.toLowerCase();
      let placeholder = {};
      let placeholder_val = SecretValue.unsafePlainText("<placeholder>");
      if (destinationConfig.type == "slack") {
        placeholder = {
          bot_user_oauth_token: placeholder_val,
          client_secret: placeholder_val,
          signing_secret: placeholder_val,
        };
      } else {
        placeholder = {
          placeholder_key: placeholder_val,
        };
      }

      const destName = destinationConfig.name;
      const formattedName = matanoResourceToCdkName(destName);

      this.integrationsSecretMap[destName] = new secrets.Secret(this, `${formattedName}Secret`, {
        description: `[Matano] ${destName} - (${destinationConfig.type}) alert delivery secret`,
        secretObjectValue: placeholder,
        encryptionKey: props.customerManagedKmsKey,
      });
      this.integrationsInfoMap[destName] = destinationConfig;
    }
  }
}
function validateDestinationConfig(destinationConfig: Record<string, any>) {
  // TODO
  return;
}
