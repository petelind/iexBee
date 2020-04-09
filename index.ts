import events = require('@aws-cdk/aws-events');
import targets = require('@aws-cdk/aws-events-targets');
import lambda = require('@aws-cdk/aws-lambda');
import cdk = require('@aws-cdk/core');
import path = require('path');

// import path = require('path');

export class LambdaCronStack extends cdk.Stack {
  constructor(app: cdk.App, id: string) {
    super(app, id);

    const lambdaFn = new lambda.Function(this, 'Singleton', {
      code: lambda.Code.fromAsset(path.join(__dirname, 'src')),
      handler: 'handler.lamdba_handler',
      timeout: cdk.Duration.seconds(300),
      runtime: lambda.Runtime.PYTHON_3_8,
    });

    // Run every day at 6PM UTC
    // See https://docs.aws.amazon.com/lambda/latest/dg/tutorial-scheduled-events-schedule-expressions.html
    const rule = new events.Rule(this, 'Rule', {
      schedule: events.Schedule.expression('cron(0 18 ? * MON-FRI *)')
    });

    rule.addTarget(new targets.LambdaFunction(lambdaFn));
  }
}

const app = new cdk.App();
new LambdaCronStack(app, 'LambdaCronExample');
app.synth();