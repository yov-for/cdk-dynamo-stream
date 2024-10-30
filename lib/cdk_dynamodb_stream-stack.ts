import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { AttributeType, BillingMode, StreamViewType, Table, TableClass } from 'aws-cdk-lib/aws-dynamodb'
// import { Role } from 'aws-cdk-lib/aws-iam'
import { Code, FilterCriteria, FilterRule, Function, Runtime, StartingPosition } from 'aws-cdk-lib/aws-lambda';
import { DynamoEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { Bucket, BucketEncryption } from 'aws-cdk-lib/aws-s3';
// import * as sqs from 'aws-cdk-lib/aws-sqs';

export class CdkDynamodbStreamStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // The code that defines your stack goes here

    const myTable = new Table(this, 'test-table', {
      tableName: 'my-test-table',
      partitionKey: { name: 'tc_date', type: AttributeType.STRING },
      tableClass: TableClass.STANDARD_INFREQUENT_ACCESS,
      billingMode: BillingMode.PAY_PER_REQUEST,
      stream: StreamViewType.NEW_IMAGE,
      deletionProtection: false
    });

    const s3Destiny = new Bucket(this, 's3Destiny', {
      bucketName: 's3destiny',
      encryption: BucketEncryption.S3_MANAGED,
      autoDeleteObjects: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY      
    })

    const fnNewItemHandler = new Function(this, 'fnNewItemHandler', {
      functionName: 'fnNewItemHandler',
      runtime: Runtime.PYTHON_3_12,
      code: Code.fromAsset('lambda'),
      handler: 'new-item-handler.handler',
      environment: {
        'BUCKET_TARGET': s3Destiny.bucketName
      }
    });

    fnNewItemHandler.addEventSource(new DynamoEventSource(myTable, {
      startingPosition: StartingPosition.LATEST,
      filters: [FilterCriteria.filter({ eventName: FilterRule.isEqual('INSERT') })],
      batchSize: 1
    }))



    s3Destiny.grantReadWrite(fnNewItemHandler)
    // example resource
    // const queue = new sqs.Queue(this, 'CdkDynamodbStreamQueue', {
    //   visibilityTimeout: cdk.Duration.seconds(300)
    // });
  }
}
