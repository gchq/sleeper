#!/bin/sh

exec /usr/bin/java -cp /function/lambda.jar:/function/aws-lambda-java-runtime.jar com.amazonaws.services.lambda.runtime.api.client.AWSLambda "$@"
