import * as cdk from '@aws-cdk/core';
import {AttributeType, BillingMode, Table} from '@aws-cdk/aws-dynamodb';
import * as lambda from '@aws-cdk/aws-lambda';
import * as tasks from '@aws-cdk/aws-stepfunctions-tasks';
import {CfnParameter, Duration} from "@aws-cdk/core";
import {Vpc, PrivateSubnet} from "@aws-cdk/aws-ec2";
import {Choice, Condition, StateMachine, Succeed} from "@aws-cdk/aws-stepfunctions";

export class CdkStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props: cdk.StackProps) {
    super(scope, id, props);

    /**
     * Stack parameters
     */
    const vpcId = new CfnParameter(this, "vpcId", {type: "String"});
    const subnetId1 = new CfnParameter(this, "subnetId1", {type: "String"});
    const subnetId2 = new CfnParameter(this, "subnetId2", {type: "String"});
    const subnetId3 = new CfnParameter(this, "subnetId3", {type: "String"});
    const izanamiHost = new CfnParameter(this, "izanamiHost", {type: "String"});
    const izanamiClientId = new CfnParameter(this, "izanamiClientId", {type: "String", noEcho: true});
    const izanamiClientSecret = new CfnParameter(this, "izanamiClientSecret", {type: "String", noEcho: true});


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

    const insertLambda = new lambda.Function(this, 'insertLambda', {
      runtime: lambda.Runtime.JAVA_8,
      code: lambda.Code.fromAsset('target/scala-2.13/article-akkastream-assembly-0.1.jar'),
      handler: 'fr.glc.articles.akkastream.lambda.InserterHandler::handler',
      functionName: "akkastream-Inserter",
      timeout: Duration.minutes(10),
      memorySize: 1024,
      environment: {
        TABLE_NAME: dynamodb.tableName
      }
    });


    const updateLambda = new lambda.Function(this, 'updateLambda', {
      runtime: lambda.Runtime.JAVA_8,
      code: lambda.Code.fromAsset('target/scala-2.13/article-akkastream-assembly-0.1.jar'),
      handler: 'fr.glc.articles.akkastream.lambda.UpdaterHandler::handler',
      functionName: "akkastream-Updater",
      timeout: Duration.minutes(10),
      memorySize: 1024,
      environment: {
        TABLE_NAME: dynamodb.tableName,
        IZANAMI_HOST: izanamiHost.valueAsString,
        IZANAMI_ID: izanamiClientId.valueAsString,
        IZANAMI_SECRET: izanamiClientSecret.valueAsString,
      },
      vpc: Vpc.fromVpcAttributes(this, "vpc", {
        vpcId: vpcId.valueAsString,
        availabilityZones: [
            "eu-west-1a",
            "eu-west-1b",
            "eu-west-1c"
        ]
      }),
      vpcSubnets: {
        subnets: [
          PrivateSubnet.fromPrivateSubnetAttributes(this, "subnet1", {subnetId: subnetId1.valueAsString}),
          PrivateSubnet.fromPrivateSubnetAttributes(this, "subnet2", {subnetId: subnetId2.valueAsString}),
          PrivateSubnet.fromPrivateSubnetAttributes(this, "subnet3", {subnetId: subnetId3.valueAsString})
        ]
      }
    });

    dynamodb.grantWriteData(insertLambda);
    dynamodb.grantReadData(updateLambda);
    dynamodb.grantWriteData(updateLambda);

    /**
     * Step function
     */
    const updateTask = new tasks.LambdaInvoke(this, "UpdateDynamoTask", {lambdaFunction: updateLambda, outputPath: "$.Payload"})

    const definition = updateTask.next(new Choice(this, "IsFinished")
        .when(Condition.booleanEquals("$.finished", true), new Succeed(this, "Success"))
        .otherwise(updateTask)
    );

    new StateMachine(this, 'UpdateDynamoStateMachine', {
      definition,
      stateMachineName: "akkastream-update"
    });

  }
}
