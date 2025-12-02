#!/bin/sh

if [ -z "${AWS_LAMBDA_RUNTIME_API}" ]; then
  exec /usr/local/bin/aws-lambda-rie /usr/bin/java -cp /function/lambda.jar:/function/aws-lambda-java-runtime.jar com.amazonaws.services.lambda.runtime.api.client.AWSLambda "$@"
else
  exec /usr/bin/java -cp /function/lambda.jar:/function/aws-lambda-java-runtime.jar com.amazonaws.services.lambda.runtime.api.client.AWSLambda "$@"
fi
