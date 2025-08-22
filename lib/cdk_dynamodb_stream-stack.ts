import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { AttributeType, BillingMode, StreamViewType, Table, TableClass } from 'aws-cdk-lib/aws-dynamodb'
// import { Role } from 'aws-cdk-lib/aws-iam'
import { Code, DockerImageCode, DockerImageFunction, FilterCriteria, FilterRule, Function, Runtime, StartingPosition } from 'aws-cdk-lib/aws-lambda';
import { DynamoEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { ArnPrincipal, Effect, PolicyStatement, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { LambdaFunction } from 'aws-cdk-lib/aws-events-targets';
import { Rule, Schedule } from 'aws-cdk-lib/aws-events';
// import * as sqs from 'aws-cdk-lib/aws-sqs';

interface LambdaDynamoDBS3RoleStackProps extends cdk.StackProps {
  environment: string;
  crossAccountBucketName: string;
  crossAccountId: string;
  RoleName: string;
}

export class CdkDynamodbStreamStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: LambdaDynamoDBS3RoleStackProps) {
    super(scope, id, props);

    const commonRole = new Role(this, 'CommonRole', {
      assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
      roleName: 'CommonS3KmsRole'
    });

    const s3Policy = new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ['s3:PutObject', 's3:PutObjectAcl'],
      resources: [`arn:aws:s3:::${props.crossAccountBucketName}/*`],
    });

    const kmsPolicy = new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ['kms:Encrypt', 'kms:GenerateDataKey'],
      resources: ['*'],
    });

    const stsPolicy = new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ['sts:AssumeRole'],
      resources: ['*'],
    });

    commonRole.addToPolicy(s3Policy);
    commonRole.addToPolicy(kmsPolicy);
    commonRole.addToPolicy(stsPolicy);

    const myTable = new Table(this, `${props.environment}-dev-tc_contable`, {
      tableName: `${props.environment}-dev-tc_contable`,
      partitionKey: { name: 'pk', type: AttributeType.STRING },
      sortKey: { name: 'tc_date', type: AttributeType.STRING },
      tableClass: TableClass.STANDARD_INFREQUENT_ACCESS,
      billingMode: BillingMode.PAY_PER_REQUEST,
      stream: StreamViewType.NEW_IMAGE,
      deletionProtection: false,
      removalPolicy: cdk.RemovalPolicy.RETAIN
    });
    
    const fnDocketScrapper = new DockerImageFunction(this, `${props.environment}-dev-tc_scrapper`, {
      code: DockerImageCode.fromImageAsset('lambda/sbs_scrapper'),
      functionName: `${props.environment}-dev-tc_scrapper`,
      memorySize: 512,
      timeout: cdk.Duration.seconds(30),
      environment: {
        'TABLE_NAME': myTable.tableName,
        'PATH_SCRAP_WEB': 'https://www.sbs.gob.pe/app/pp/SISTIP_PORTAL/Paginas/Publicacion/TipoCambioContable.aspx',
        'TAKEN_BACK_DAYS': '10'
      }
    });
    
    myTable.grantWriteData(fnDocketScrapper);

    const eventFnDockerScrapper = new Rule(this, 'Rule', {
      ruleName: `${props.environment}-dev-tc_scrapper_rule`,
      description: "Schedule a Lambda that creates a report every 1st of the month",
      schedule: Schedule.cron({
          year: "*",
          month: "*",
          day: "*",
          hour: "14",
          minute: "0",
      }),
      targets: [new LambdaFunction(fnDocketScrapper)],
    });

    const fnNewItemHandler = new Function(this, `${props.environment}-dev-ddb_stream_handler`, {
      functionName: `${props.environment}-dev-ddb_stream_handler`,
      runtime: Runtime.PYTHON_3_12,
      code: Code.fromAsset('lambda/new-item_handler'),
      handler: 'main.handler',
      environment: {
        'BUCKET_TARGET': `${props.crossAccountBucketName}`,
        'ROLE_TO_ASSUME': commonRole.roleArn
        // 'ROLE_TO_ASSUME': 'ROLE_TO_ASSUME_ARN'
      },
      timeout: cdk.Duration.seconds(10)
    });

    fnNewItemHandler.addEventSource(new DynamoEventSource(myTable, {
      startingPosition: StartingPosition.LATEST,
      filters: [FilterCriteria.filter({ eventName:  ["INSERT", "MODIFY"] })],
      batchSize: 1
    }));

    const dynamoStreamPolicy = new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ['dynamodb:DescribeStream', 'dynamodb:GetRecords', 'dynamodb:GetShardIterator', 'dynamodb:ListStreams'],
      resources: [myTable.tableStreamArn!],
    });

    const assumePolicy = new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ['sts:AssumeRole'],
      // resources: [`arn:aws:iam::${props.env?.account}:role/${props.RoleName}`],
      resources: [commonRole.roleArn],
    });

    fnNewItemHandler.addToRolePolicy(dynamoStreamPolicy);
    // fnNewItemHandler.addToRolePolicy(s3Policy);
    // fnNewItemHandler.addToRolePolicy(kmsPolicy);
    fnNewItemHandler.addToRolePolicy(assumePolicy);

    commonRole.assumeRolePolicy?.addStatements(new PolicyStatement({
      effect: Effect.ALLOW,
      principals: [new ArnPrincipal(fnNewItemHandler.role?.roleArn??'')],
      actions: ['sts:AssumeRole']
    }));
  }
}
