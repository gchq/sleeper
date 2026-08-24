#!/bin/sh

exec /usr/local/bin/aws-lambda-rie /usr/bin/java -cp /function/lambda.jar:/function/aws-lambda-java-runtime.jar com.amazonaws.services.lambda.runtime.api.client.AWSLambda "$@"
