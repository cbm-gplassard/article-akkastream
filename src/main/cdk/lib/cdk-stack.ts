import * as cdk from '@aws-cdk/core';
import {AttributeType, BillingMode, Table} from '@aws-cdk/aws-dynamodb';

export class CdkStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props: cdk.StackProps) {
    super(scope, id, props);

    const dynamodb = new Table(this, "table", {
      billingMode: BillingMode.PROVISIONED,
      readCapacity: 1,
      writeCapacity: 1,
      tableName: "akkastream",
      partitionKey: {
        name: "pk",
        type: AttributeType.STRING
      }
    });

  }
}
