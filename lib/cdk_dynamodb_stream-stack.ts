import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { AttributeType, BillingMode, StreamViewType, Table, TableClass } from 'aws-cdk-lib/aws-dynamodb'
// import { Role } from 'aws-cdk-lib/aws-iam'
import { Code, FilterCriteria, FilterRule, Function, Runtime, StartingPosition } from 'aws-cdk-lib/aws-lambda';
import { DynamoEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { Bucket, BucketEncryption } from 'aws-cdk-lib/aws-s3';
import { Effect, PolicyStatement, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
// import * as sqs from 'aws-cdk-lib/aws-sqs';

interface LambdaDynamoDBS3RoleStackProps extends cdk.StackProps {
  s3BucketArn: string;           // ARN of the S3 bucket in the other account
  s3BucketAccountId: string;     // Account ID for the S3 bucket
}

export class CdkDynamodbStreamStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: LambdaDynamoDBS3RoleStackProps) {
    super(scope, id, props);

    const myTable = new Table(this, 'test-table', {
      tableName: 'TABLE_NAME',
      partitionKey: { name: 'tc_date', type: AttributeType.STRING },
      tableClass: TableClass.STANDARD_INFREQUENT_ACCESS,
      billingMode: BillingMode.PAY_PER_REQUEST,
      stream: StreamViewType.NEW_IMAGE,
      deletionProtection: false
    });

    const fnNewItemHandler = new Function(this, 'fnNewItemHandler', {
      functionName: 'fnNewItemHandler',
      runtime: Runtime.PYTHON_3_12,
      code: Code.fromAsset('lambda/new-item_handler'),
      handler: 'main.handler',
      environment: {
        'BUCKET_TARGET': 'BUCKET_TARGET'
      }
    });

    fnNewItemHandler.addEventSource(new DynamoEventSource(myTable, {
      startingPosition: StartingPosition.LATEST,
      filters: [FilterCriteria.filter({ eventName: FilterRule.isEqual('INSERT') })],
      batchSize: 1
    }));
    
    const lambdaRole = new Role(this, 'LambdaDynamoDBS3Role', {
      assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
    });

    const dynamoStreamPolicy = new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ['dynamodb:DescribeStream', 'dynamodb:GetRecords', 'dynamodb:GetShardIterator', 'dynamodb:ListStreams'],
      resources: [myTable.tableStreamArn!],
    });

    const s3Policy = new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ['s3:PutObject', 's3:PutObjectAcl'],
      resources: [`${props.s3BucketArn}/*`],
    });

    fnNewItemHandler.addToRolePolicy(dynamoStreamPolicy);
    fnNewItemHandler.addToRolePolicy(s3Policy);

  }
}
