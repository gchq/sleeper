/*
 * Copyright 2022-2026 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sleeper.api;

import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Datapoint;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricStatisticsResponse;
import software.amazon.awssdk.services.cloudwatch.model.Statistic;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.DescribeRuleResponse;
import software.amazon.awssdk.services.eventbridge.model.RuleState;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.GetFunctionConfigurationResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;

public class AWSArchitectureResources {

    private AWSArchitectureResources() {}

    public record ResourcesResponse(Map<String, Resource> resources) {}
    public record Resource(String type, String name, String arn, String url, String logGroup, String status, String detail) {}

    public static Resource dynamoTable(DynamoDbClient dynamoClient, String tableName) {
        String arn = null;
        String status = "ok";
        String detail = null;
        try {
            DescribeTableResponse describe = dynamoClient.describeTable(builder -> builder.tableName(tableName));
            arn = describe.table().tableArn();
            String tableStatus = describe.table().tableStatusAsString();
            if (!"ACTIVE".equals(tableStatus)) {
                status = "warning";
                detail = tableStatus;
            }
        } catch (RuntimeException e) {
            status = "unknown";
            detail = "Status unavailable";
        }

        return new Resource("DynamoDB::Table", tableName, arn, null, null, status, detail);
    }

    public static Resource eventBridgeRule(EventBridgeClient eventBridgeClient, String ruleName) {
        String arn = null;
        String status = "ok";
        String detail = null;

        try {
            DescribeRuleResponse describe = eventBridgeClient.describeRule(builder -> builder.name(ruleName));
            arn = describe.arn();
            if (describe.state() == RuleState.DISABLED) {
                status = "error";
                detail = "Disabled";
            } else {
                detail = "Enabled";
            }
        } catch (RuntimeException e) {
            status = "unknown";
            detail = "Status unavailable";
        }

        return new Resource("EventBridge::Rule", ruleName, arn, null, null, status, detail);
    }

    public static Resource sqsQueue(SqsClient sqsClient, String url, String arn, boolean isDlq) {
        String name = arn == null || arn.isEmpty() ? null : Arn.fromString(arn).resourceAsString();
        String status = "ok";
        String detail = null;

        if (isDlq) {
            try {
                GetQueueAttributesResponse attrs = sqsClient.getQueueAttributes(builder -> builder
                        .queueUrl(url)
                        .attributeNames(
                                QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                                QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE));
                long visible = attributeLong(attrs, QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES);
                long inFlight = attributeLong(attrs, QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE);
                long total = visible + inFlight;
                status = total > 0 ? "error" : "ok";
                detail = total > 0 ? total + " message" + (total == 1 ? "" : "s") + " in dead-letter queue" : "Queue is empty";
            } catch (RuntimeException e) {
                status = "unknown";
                detail = "Status unavailable";
            }
        }

        return new Resource("SQS::Queue", name, arn, url, null, status, detail);
    }

    public static Resource s3Bucket(String bucketName) {
        String arn = "arn:aws:s3:::" + bucketName;
        return new Resource("S3::Bucket", bucketName, arn, null, null, "ok", null);
    }

    private static long attributeLong(GetQueueAttributesResponse attrs, QueueAttributeName name) {
        String value = attrs.attributes().get(name);
        if (value == null) {
            return 0;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    public static Resource lambdaFunction(LambdaClient lambdaClient, CloudWatchClient cloudWatchClient, String name) {
        String functionName = normaliseLambdaFunctionName(name);

        String arn = null;
        String logGroupName = null;
        String status = "ok";
        String detail = null;

        try {
            GetFunctionConfigurationResponse config = lambdaClient.getFunctionConfiguration(builder -> builder.functionName(functionName));
            arn = config.functionArn();
            if (config.loggingConfig() != null && config.loggingConfig().logGroup() != null) {
                logGroupName = config.loggingConfig().logGroup();
            }
        } catch (RuntimeException e) {
            status = "unknown";
            detail = "Status unavailable";
        }

        try {
            long errors = Math.round(lambdaMetricSum(cloudWatchClient, functionName, "Errors"));
            long throttles = Math.round(lambdaMetricSum(cloudWatchClient, functionName, "Throttles"));
            long invocations = Math.round(lambdaMetricSum(cloudWatchClient, functionName, "Invocations"));
            long successes = Math.max(0, invocations - errors);

            if (errors > 0 && successes == 0) {
                status = "error";
                detail = errors + " error" + (errors == 1 ? "" : "s") + " in last hour";
            } else if (errors > 0) {
                status = "warning";
                detail = errors + " error" + (errors == 1 ? "" : "s") + " in last hour";
            } else if (throttles > 0) {
                status = "warning";
                detail = throttles + " throttle" + (throttles == 1 ? "" : "s") + " in last hour";
            } else {
                detail = "Healthy";
            }
        } catch (RuntimeException e) {
            status = "unknown";
            detail = "Status unavailable";
        }

        return new Resource("Lambda::Function", functionName, arn, null, logGroupName, status, detail);
    }

    private static double lambdaMetricSum(CloudWatchClient cloudWatchClient, String functionName, String metricName) {
        Instant end = Instant.now();
        Instant start = end.minus(1, ChronoUnit.HOURS);
        GetMetricStatisticsResponse response = cloudWatchClient.getMetricStatistics(builder -> builder
                .namespace("AWS/Lambda")
                .metricName(metricName)
                .dimensions(Dimension.builder().name("FunctionName").value(functionName).build())
                .startTime(start)
                .endTime(end)
                .period((int) ChronoUnit.HOURS.getDuration().getSeconds())
                .statistics(Statistic.SUM));
        return response.datapoints().stream()
                .mapToDouble(Datapoint::sum)
                .sum();
    }

    /**
     * Reduces a Lambda function reference to its bare name. Accepts a plain name, a function ARN, or a
     * URL-encoded value, and strips any trailing version or alias qualifier (e.g. ":1", ":LIVE").
     */
    private static String normaliseLambdaFunctionName(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        String decoded = URLDecoder.decode(value, StandardCharsets.UTF_8);
        String name = decoded;
        int functionMarker = decoded.indexOf(":function:");
        if (functionMarker >= 0) {
            name = decoded.substring(functionMarker + ":function:".length());
        }
        // Drop any :version or :alias qualifier left on the name.
        int qualifier = name.indexOf(':');
        if (qualifier >= 0) {
            name = name.substring(0, qualifier);
        }
        return name.isBlank() ? null : name;
    }

}
