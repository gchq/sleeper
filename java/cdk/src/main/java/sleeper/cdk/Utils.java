/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.cdk;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.internal.BucketNameUtils;
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
import software.amazon.awscdk.services.logs.LogGroup;
import software.amazon.awscdk.services.logs.RetentionDays;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.core.SleeperVersion;
import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.instance.InstanceProperty;
import sleeper.core.properties.local.LoadLocalProperties;
import sleeper.core.properties.table.TableProperties;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.lang.String.format;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.LOG_RETENTION_IN_DAYS;
import static sleeper.core.properties.instance.CommonProperty.RETAIN_INFRA_AFTER_DESTROY;
import static sleeper.core.properties.instance.CommonProperty.STACK_TAG_NAME;
import static sleeper.core.properties.instance.DashboardProperty.DASHBOARD_TIME_WINDOW_MINUTES;
import static sleeper.core.properties.instance.LoggingLevelsProperty.APACHE_LOGGING_LEVEL;
import static sleeper.core.properties.instance.LoggingLevelsProperty.AWS_LOGGING_LEVEL;
import static sleeper.core.properties.instance.LoggingLevelsProperty.LOGGING_LEVEL;
import static sleeper.core.properties.instance.LoggingLevelsProperty.PARQUET_LOGGING_LEVEL;
import static sleeper.core.properties.instance.LoggingLevelsProperty.ROOT_LOGGING_LEVEL;

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

    public static Map<String, String> createDefaultEnvironment(InstanceProperties instanceProperties) {
        Map<String, String> environmentVariables = new HashMap<>();
        environmentVariables.put(CONFIG_BUCKET.toEnvironmentVariable(),
                instanceProperties.get(CONFIG_BUCKET));

        environmentVariables.put("JAVA_TOOL_OPTIONS", createToolOptions(instanceProperties,
                LOGGING_LEVEL,
                ROOT_LOGGING_LEVEL,
                APACHE_LOGGING_LEVEL,
                PARQUET_LOGGING_LEVEL,
                AWS_LOGGING_LEVEL));

        return environmentVariables;
    }

    private static String createToolOptions(InstanceProperties instanceProperties, InstanceProperty... propertyNames) {
        StringBuilder sb = new StringBuilder();
        Arrays.stream(propertyNames)
                .filter(s -> instanceProperties.get(s) != null)
                .forEach(s -> sb.append("-D").append(s.getPropertyName())
                        .append("=").append(instanceProperties.get(s)).append(" "));

        return sb.toString();
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
        return properties.get(ID)
                .toLowerCase(Locale.ROOT)
                .replace(".", "-");
    }

    /**
     * Configures a log group with the specified number of days. Valid values are taken from
     * <a href=
     * "https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-logs-loggroup.html">here</a>.
     * A value of -1 represents an infinite number of days.
     *
     * @param  numberOfDays number of days you want to retain the logs
     * @return              The RetentionDays equivalent
     */
    public static LogGroup createLogGroupWithRetentionDays(Construct scope, String id, int numberOfDays) {
        return LogGroup.Builder.create(scope, id)
                .retention(getRetentionDays(numberOfDays))
                .build();
    }

    public static LogGroup createLambdaLogGroup(
            Construct scope, String id, String functionName, InstanceProperties instanceProperties) {
        return LogGroup.Builder.create(scope, id)
                .logGroupName(functionName)
                .retention(getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS)))
                .build();
    }

    public static LogGroup createCustomResourceProviderLogGroup(
            Construct scope, String id, String functionName, InstanceProperties instanceProperties) {
        return LogGroup.Builder.create(scope, id)
                .logGroupName(functionName + "-provider")
                .retention(getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS)))
                .build();
    }

    public static LogDriver createECSContainerLogDriver(Construct scope, InstanceProperties instanceProperties, String id) {
        String logGroupName = String.join("-", "sleeper", cleanInstanceId(instanceProperties), id);
        AwsLogDriverProps logDriverProps = AwsLogDriverProps.builder()
                .streamPrefix(logGroupName)
                .logGroup(LogGroup.Builder.create(scope, id)
                        .logGroupName(logGroupName)
                        .retention(getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS)))
                        .build())
                .build();
        return LogDriver.awsLogs(logDriverProps);
    }

    private static RetentionDays getRetentionDays(int numberOfDays) {
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

    public static <T extends InstanceProperties> T loadInstanceProperties(Function<Properties, T> properties, Construct scope) {
        return loadInstanceProperties(properties, tryGetContext(scope));
    }

    public static <T extends InstanceProperties> T loadInstanceProperties(
            Function<Properties, T> constructor, Function<String, String> tryGetContext) {
        Path propertiesFile = Path.of(tryGetContext.apply("propertiesfile"));
        T properties = LoadLocalProperties.loadInstancePropertiesNoValidation(constructor, propertiesFile);

        if (!"false".equalsIgnoreCase(tryGetContext.apply("validate"))) {
            properties.validate();
            if (!BucketNameUtils.isValidV2BucketName(properties.get(ID))) {
                throw new IllegalArgumentException(
                        "Sleeper instance ID is not valid as part of an S3 bucket name: " + properties.get(ID));
            }
        }
        if ("true".equalsIgnoreCase(tryGetContext.apply("newinstance"))) {
            new NewInstanceValidator(AmazonS3ClientBuilder.defaultClient(),
                    AmazonDynamoDBClientBuilder.defaultClient()).validate(properties, propertiesFile);
        }
        String deployedVersion = properties.get(VERSION);
        String localVersion = SleeperVersion.getVersion();
        CdkDefinedInstanceProperty.getAll().forEach(properties::unset);

        if (!"true".equalsIgnoreCase(tryGetContext.apply("skipVersionCheck"))
                && deployedVersion != null
                && !localVersion.equals(deployedVersion)) {
            throw new MismatchedVersionException(format("Local version %s does not match deployed version %s. " +
                    "Please upgrade/downgrade to make these match",
                    localVersion, deployedVersion));
        }
        properties.set(VERSION, localVersion);
        return properties;
    }

    public static Stream<TableProperties> getAllTableProperties(
            InstanceProperties instanceProperties, Construct scope) {
        return getAllTableProperties(instanceProperties, tryGetContext(scope));
    }

    public static Stream<TableProperties> getAllTableProperties(
            InstanceProperties instanceProperties, Function<String, String> tryGetContext) {
        return LoadLocalProperties.loadTablesFromInstancePropertiesFile(
                instanceProperties, Path.of(tryGetContext.apply("propertiesfile")));
    }

    private static Function<String, String> tryGetContext(Construct scope) {
        return key -> (String) scope.getNode().tryGetContext(key);
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
