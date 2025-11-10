/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.cdk.util;

import software.amazon.awscdk.Duration;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.Tags;
import software.amazon.awscdk.services.cloudwatch.Alarm;
import software.amazon.awscdk.services.cloudwatch.ComparisonOperator;
import software.amazon.awscdk.services.cloudwatch.IMetric;
import software.amazon.awscdk.services.cloudwatch.MetricOptions;
import software.amazon.awscdk.services.cloudwatch.TreatMissingData;
import software.amazon.awscdk.services.cloudwatch.actions.SnsAction;
import software.amazon.awscdk.services.ecs.AwsLogDriverProps;
import software.amazon.awscdk.services.ecs.LogDriver;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.ManagedPolicy;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.logs.ILogGroup;
import software.amazon.awscdk.services.logs.RetentionDays;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.sqs.Queue;
import software.amazon.awscdk.services.stepfunctions.LogLevel;
import software.amazon.awscdk.services.stepfunctions.LogOptions;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.internal.BucketUtils;
import software.constructs.Construct;

import sleeper.core.SleeperVersion;
import sleeper.core.deploy.DeployInstanceConfiguration;
import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.local.LoadLocalProperties;

import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.RETAIN_INFRA_AFTER_DESTROY;
import static sleeper.core.properties.instance.CommonProperty.STACK_TAG_NAME;
import static sleeper.core.properties.instance.MetricsProperty.DASHBOARD_TIME_WINDOW_MINUTES;

/**
 * Collection of utility methods related to the CDK deployment.
 */
public class Utils {

    /**
     * Region environment variable for setting in EC2 based ECS containers.
     */
    public static final String AWS_REGION = "AWS_REGION";

    private static final Pattern NUM_MATCH = Pattern.compile("^(\\d+)(\\D*)$");

    private Utils() {
        // Prevents instantiation
    }

    /**
     * Returns a cleaned up version of the Sleeper instance ID for use in resource names. Note that the instance ID
     * has a maximum length of 20 characters. See
     * {@link sleeper.core.properties.instance.CommonProperty#ID_MAX_LENGTH}.
     *
     * @param  properties the instance properties
     * @return            the cleaned up instance ID
     */
    public static String cleanInstanceId(InstanceProperties properties) {
        return cleanInstanceId(properties.get(ID));
    }

    public static String cleanInstanceId(String instanceId) {
        return instanceId.toLowerCase(Locale.ROOT)
                .replace(".", "-");
    }

    public static LogDriver createECSContainerLogDriver(ILogGroup logGroup) {
        return LogDriver.awsLogs(AwsLogDriverProps.builder()
                .streamPrefix(logGroup.getLogGroupName())
                .logGroup(logGroup)
                .build());
    }

    public static LogOptions createStateMachineLogOptions(ILogGroup logGroup) {
        return LogOptions.builder()
                .destination(logGroup)
                .level(LogLevel.ALL)
                .includeExecutionData(true)
                .build();
    }

    public static RetentionDays getRetentionDays(int numberOfDays) {
        switch (numberOfDays) {
            case -1:
                return RetentionDays.INFINITE;
            case 1:
                return RetentionDays.ONE_DAY;
            case 3:
                return RetentionDays.THREE_DAYS;
            case 5:
                return RetentionDays.FIVE_DAYS;
            case 7:
                return RetentionDays.ONE_WEEK;
            case 14:
                return RetentionDays.TWO_WEEKS;
            case 30:
                return RetentionDays.ONE_MONTH;
            case 60:
                return RetentionDays.TWO_MONTHS;
            case 90:
                return RetentionDays.THREE_MONTHS;
            case 120:
                return RetentionDays.FOUR_MONTHS;
            case 150:
                return RetentionDays.FIVE_MONTHS;
            case 180:
                return RetentionDays.SIX_MONTHS;
            case 365:
                return RetentionDays.ONE_YEAR;
            case 400:
                return RetentionDays.THIRTEEN_MONTHS;
            case 545:
                return RetentionDays.EIGHTEEN_MONTHS;
            case 731:
                return RetentionDays.TWO_YEARS;
            case 1827:
                return RetentionDays.FIVE_YEARS;
            case 3653:
                return RetentionDays.TEN_YEARS;
            default:
                throw new IllegalArgumentException("Invalid number of days; see https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-logs-loggroup.html for valid options");
        }
    }

    public static <T extends InstanceProperties> T loadInstanceProperties(
            Function<Properties, T> constructor, CdkContext context) {
        Path propertiesFile = Path.of(context.tryGetContext("propertiesfile"));
        T properties = LoadLocalProperties.loadInstancePropertiesNoValidation(constructor, propertiesFile);
        afterInstancePropertiesLoad(properties, context);
        return properties;
    }

    public static DeployInstanceConfiguration loadDeployInstanceConfiguration(CdkContext context) {
        Path propertiesFile = Path.of(context.tryGetContext("propertiesfile"));
        DeployInstanceConfiguration configuration = DeployInstanceConfiguration.fromLocalConfiguration(propertiesFile);
        afterInstancePropertiesLoad(configuration.getInstanceProperties(), context);
        return configuration;
    }

    private static void afterInstancePropertiesLoad(InstanceProperties properties, CdkContext context) {
        if (!"false".equalsIgnoreCase(context.tryGetContext("validate"))) {
            properties.validate();
            try {
                BucketUtils.isValidDnsBucketName(properties.get(ID), true);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        "Sleeper instance ID is not valid as part of an S3 bucket name: " + properties.get(ID),
                        e);
            }
        }
        if ("true".equalsIgnoreCase(context.tryGetContext("newinstance"))) {
            try (S3Client s3Client = S3Client.create(); DynamoDbClient dynamoClient = DynamoDbClient.create()) {
                new NewInstanceValidator(s3Client, dynamoClient).validate(properties);
            }
        }
        String deployedVersion = properties.get(VERSION);
        String localVersion = SleeperVersion.getVersion();
        CdkDefinedInstanceProperty.getAll().forEach(properties::unset);

        if (!"true".equalsIgnoreCase(context.tryGetContext("skipVersionCheck"))
                && deployedVersion != null
                && !localVersion.equals(deployedVersion)) {
            throw new MismatchedVersionException(String.format("Local version %s does not match deployed version %s. " +
                    "Please upgrade/downgrade to make these match",
                    localVersion, deployedVersion));
        }
        properties.set(VERSION, localVersion);
    }

    public static boolean shouldDeployPaused(Construct scope) {
        return "true".equalsIgnoreCase((String) scope.getNode().tryGetContext("deployPaused"));
    }

    public static void addStackTagIfSet(Stack stack, InstanceProperties properties) {
        Optional.ofNullable(properties.get(STACK_TAG_NAME))
                .ifPresent(tagName -> Tags.of(stack).add(tagName, stack.getNode().getId()));
    }

    public static RemovalPolicy removalPolicy(InstanceProperties properties) {
        if (properties.getBoolean(RETAIN_INFRA_AFTER_DESTROY)) {
            return RemovalPolicy.RETAIN;
        } else {
            return RemovalPolicy.DESTROY;
        }
    }

    /**
     * Normalises EC2 instance size strings to match enum identifiers. They can then be looked up in the
     * {@link software.amazon.awscdk.services.ec2.InstanceSize} enum. Java identifiers can't start with a number, so
     * "2xlarge" becomes "xlarge2".
     *
     * @param  size the human readable size
     * @return      the internal enum name
     */
    public static String normaliseSize(String size) {
        if (size == null) {
            return null;
        }
        Matcher sizeMatch = NUM_MATCH.matcher(size);
        if (sizeMatch.matches()) {
            // Match occurred so switch the capture groups
            return sizeMatch.group(2) + sizeMatch.group(1);
        } else {
            return size;
        }
    }

    public static void createAlarmForDlq(Construct scope, String id, String description, Queue dlq, Topic topic) {
        Alarm alarm = Alarm.Builder
                .create(scope, id)
                .alarmName(dlq.getQueueName())
                .alarmDescription(description)
                .metric(dlq.metricApproximateNumberOfMessagesVisible()
                        .with(MetricOptions.builder().statistic("Sum").period(Duration.seconds(60)).build()))
                .comparisonOperator(ComparisonOperator.GREATER_THAN_THRESHOLD)
                .threshold(0)
                .evaluationPeriods(1)
                .datapointsToAlarm(1)
                .treatMissingData(TreatMissingData.IGNORE)
                .build();
        alarm.addAlarmAction(new SnsAction(topic));
    }

    public static void grantInvokeOnPolicy(IFunction function, ManagedPolicy policy) {
        // IFunction.grantInvoke does not work with a ManagedPolicy at time of writing.
        // It tries to set it as a Principal, which you can't do with a ManagedPolicy.
        policy.addStatements(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of("lambda:InvokeFunction"))
                .resources(List.of(function.getFunctionArn()))
                .build());
    }

    public static IMetric createErrorMetric(String label, Queue errorQueue, InstanceProperties instanceProperties) {
        int timeWindowInMinutes = instanceProperties.getInt(DASHBOARD_TIME_WINDOW_MINUTES);
        return errorQueue.metricApproximateNumberOfMessagesVisible(
                MetricOptions.builder().label(label).period(Duration.minutes(timeWindowInMinutes)).statistic("Sum").build());
    }
}
