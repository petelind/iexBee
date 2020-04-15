import events = require('@aws-cdk/aws-events');
import targets = require('@aws-cdk/aws-events-targets');
import lambda = require('@aws-cdk/aws-lambda');
import dynamodb = require('@aws-cdk/aws-dynamodb');
import cdk = require('@aws-cdk/core');
import path = require('path');

export class LambdaCronStack extends cdk.Stack {
  constructor(app: cdk.App, id: string) {
    super(app, id);

    const ApiTOKEN = new cdk.CfnParameter(this, "ApiTOKEN", {type: "String",
    description: "iexcloud api token"});
    const TestENVIRONMENT = new cdk.CfnParameter(this, "TestENVIRONMENT", {type: "String",
    description: "iexcloud test environment toggle", default: 'False'});
    const TestSTOCKS = new cdk.CfnParameter(this, "TestSTOCKS", {type: "String",
    description: "iexcloud test stocks environment toggle", default: 'False'});
    const JsonLOGS = new cdk.CfnParameter(this, "JsonLOGS", {type: "String",
    description: "iexcloud json logs environment toggle", default: 'True'});
    
    const layer = new lambda.LayerVersion(this, 'Dependencies', {
      code: lambda.Code.fromAsset(path.join(__dirname, '.build/reqs')),
      compatibleRuntimes: [lambda.Runtime.PYTHON_3_8],
      description: 'ConsumingAPI dependecies',
    });

    
    const dynamoTable = new dynamodb.Table(this, 'CDKIexSnapshot', {
      partitionKey: {
        name: 'symbol',
        type: dynamodb.AttributeType.STRING
      },
      tableName: 'CDKIexSnapshot',

      // The default removal policy is RETAIN, which means that cdk destroy will not attempt to delete
      // the new table, and it will remain in your account until manually deleted. By setting the policy to 
      // DESTROY, cdk destroy will delete the table (even if it has data in it)
      removalPolicy: cdk.RemovalPolicy.DESTROY, // NOT recommended for production code
    });

    const lambdaFn = new lambda.Function(this, 'ConsumingApi', {
      code: lambda.Code.fromAsset(path.join(__dirname, 'src')),
      handler: 'handler.lambda_handler',
      timeout: cdk.Duration.seconds(300),
      runtime: lambda.Runtime.PYTHON_3_8,
      layers: [layer],
      environment: {
        TABLE: dynamoTable.tableName,
        TEST_ENVIRONMENT: TestENVIRONMENT.toString(),
        API_TOKEN: ApiTOKEN.toString(),
        TEST_STOCKS: TestSTOCKS.toString(),
        JSON_LOGS: JsonLOGS.toString(),
      },
    });
    lambdaFn.currentVersion.addAlias('live');
    // Run every day at 6PM UTC
    // See https://docs.aws.amazon.com/lambda/latest/dg/tutorial-scheduled-events-schedule-expressions.html
    const rule = new events.Rule(this, 'Rule', {
      schedule: events.Schedule.expression('cron(0 18 ? * MON-FRI *)')
    });

    dynamoTable.grantReadWriteData(lambdaFn);
    rule.addTarget(new targets.LambdaFunction(lambdaFn));
  }
}

const app = new cdk.App();
new LambdaCronStack(app, 'LambdaCronExample');
app.synth();