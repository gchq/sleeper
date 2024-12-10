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

package sleeper.core.properties.instance;

import sleeper.core.properties.SleeperPropertyIndex;
import sleeper.core.properties.validation.LambdaDeployType;
import sleeper.core.properties.validation.OptionalStack;
import sleeper.core.properties.validation.SleeperPropertyValueUtils;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Definitions of instance properties commonly set for any instance.
 */
public interface CommonProperty {
    int ID_MAX_LENGTH = 20;
    UserDefinedInstanceProperty ID = Index.propertyBuilder("sleeper.id")
            .description("A string to uniquely identify this deployment. This should be no longer than 20 chars. " +
                    "It should be globally unique as it will be used to name AWS resources such as S3 buckets.")
            .validationPredicate(value -> SleeperPropertyValueUtils.isNonNullNonEmptyStringWithMaxLength(value, ID_MAX_LENGTH))
            .propertyGroup(InstancePropertyGroup.COMMON)
            .editable(false).build();
    UserDefinedInstanceProperty JARS_BUCKET = Index.propertyBuilder("sleeper.jars.bucket")
            .description("The S3 bucket containing the jar files of the Sleeper components.")
            .validationPredicate(Objects::nonNull)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty USER_JARS = Index.propertyBuilder("sleeper.userjars")
            .description("A comma-separated list of the jars containing application specific iterator code. " +
                    "These jars are assumed to be in the bucket given by sleeper.jars.bucket, e.g. if that " +
                    "bucket contains two iterator jars called iterator1.jar and iterator2.jar then the " +
                    "property should be 'sleeper.userjars=iterator1.jar,iterator2.jar'.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .includedInBasicTemplate(true).build();
    UserDefinedInstanceProperty TAGS = Index.propertyBuilder("sleeper.tags")
            .description("A list of tags for the project.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true)
            .includedInTemplate(false).build();
    UserDefinedInstanceProperty STACK_TAG_NAME = Index.propertyBuilder("sleeper.stack.tag.name")
            .description("A name for a tag to identify the stack that deployed a resource. This will be set for all AWS resources, to the ID of " +
                    "the CDK stack that they are deployed under. This can be used to organise the cost explorer for billing.")
            .defaultValue("DeploymentStack")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty RETAIN_INFRA_AFTER_DESTROY = Index.propertyBuilder("sleeper.retain.infra.after.destroy")
            .description("Whether to keep the sleeper table bucket, Dynamo tables, query results bucket, etc., " +
                    "when the instance is destroyed.")
            .defaultValue("true")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true)
            .includedInBasicTemplate(true).build();
    UserDefinedInstanceProperty OPTIONAL_STACKS = Index.propertyBuilder("sleeper.optional.stacks")
            .description("The optional stacks to deploy. Not case sensitive.\n" +
                    "Valid values: " + SleeperPropertyValueUtils.describeEnumValues(OptionalStack.class))
            .defaultValue(OptionalStack.getDefaultValue())
            .validationPredicate(OptionalStack::isValid)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true)
            .includedInBasicTemplate(true)
            .ignoreEmptyValue(false).build();
    UserDefinedInstanceProperty LAMBDA_DEPLOY_TYPE = Index.propertyBuilder("sleeper.lambda.deploy.type")
            .description("The deployment type for AWS Lambda. Not case sensitive.\n" +
                    "Valid values: " + SleeperPropertyValueUtils.describeEnumValuesInLowerCase(LambdaDeployType.class))
            .defaultValue(LambdaDeployType.JAR.toString().toLowerCase(Locale.ROOT))
            .validationPredicate(LambdaDeployType::isValid)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true)
            .build();
    UserDefinedInstanceProperty ACCOUNT = Index.propertyBuilder("sleeper.account")
            .description("The AWS account number. This is the AWS account that the instance will be deployed to.")
            .validationPredicate(Objects::nonNull)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .editable(false).build();
    UserDefinedInstanceProperty REGION = Index.propertyBuilder("sleeper.region")
            .description("The AWS region to deploy to.")
            .validationPredicate(Objects::nonNull)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .editable(false).build();
    UserDefinedInstanceProperty VPC_ID = Index.propertyBuilder("sleeper.vpc")
            .description("The id of the VPC to deploy to.")
            .validationPredicate(Objects::nonNull)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .editable(false).build();
    UserDefinedInstanceProperty VPC_ENDPOINT_CHECK = Index.propertyBuilder("sleeper.vpc.endpoint.check")
            .description("Whether to check that the VPC that the instance is deployed to has an S3 endpoint. " +
                    "If there is no S3 endpoint then the NAT costs can be very significant.")
            .defaultValue("true")
            .propertyGroup(InstancePropertyGroup.COMMON).build();
    UserDefinedInstanceProperty SUBNETS = Index.propertyBuilder("sleeper.subnets")
            .description("A comma separated list of subnets to deploy to. ECS tasks will be run across multiple " +
                    "subnets. EMR clusters will be deployed in a subnet chosen when the cluster is created.")
            .validationPredicate(Objects::nonNull)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .editable(false).build();
    UserDefinedInstanceProperty FILE_SYSTEM = Index.propertyBuilder("sleeper.filesystem")
            .description("The Hadoop filesystem used to connect to S3.")
            .defaultValue("s3a://")
            .propertyGroup(InstancePropertyGroup.COMMON).build();
    UserDefinedInstanceProperty EMAIL_ADDRESS_FOR_ERROR_NOTIFICATION = Index.propertyBuilder("sleeper.errors.email")
            .description("An email address used by the TopicStack to publish SNS notifications of errors.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true)
            .includedInBasicTemplate(true).build();
    UserDefinedInstanceProperty QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.queue.visibility.timeout.seconds")
            .description("The visibility timeout on the queues used in ingest, query, etc.")
            .defaultValue("900")
            .validationPredicate(SleeperPropertyValueUtils::isValidLambdaTimeout)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty LOG_RETENTION_IN_DAYS = Index.propertyBuilder("sleeper.log.retention.days")
            .description("The length of time in days that CloudWatch logs from lambda functions, ECS containers, etc., are retained.\n" +
                    "See https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-logs-loggroup.html for valid options.\n" +
                    "Use -1 to indicate infinite retention.")
            .defaultValue("30")
            .validationPredicate(SleeperPropertyValueUtils::isValidLogRetention)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty MAXIMUM_CONNECTIONS_TO_S3 = Index.propertyBuilder("sleeper.fs.s3a.max-connections")
            .description("Used to set the value of fs.s3a.connection.maximum on the Hadoop configuration. This controls the " +
                    "maximum number of http connections to S3.\n" +
                    "See https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/performance.html")
            .defaultValue("100")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.COMMON).build();
    UserDefinedInstanceProperty S3_UPLOAD_BLOCK_SIZE = Index.propertyBuilder("sleeper.fs.s3a.upload.block.size")
            .description("Used to set the value of fs.s3a.block.size on the Hadoop configuration. Uploads to S3 " +
                    "happen in blocks, and this sets the size of blocks. If a larger value is used, then more data " +
                    "is buffered before the upload begins.\n" +
                    "See https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/performance.html")
            .defaultValue("32M")
            .validationPredicate(SleeperPropertyValueUtils::isValidHadoopLongBytes)
            .propertyGroup(InstancePropertyGroup.COMMON).build();
    UserDefinedInstanceProperty FARGATE_VERSION = Index.propertyBuilder("sleeper.fargate.version")
            .description("The version of Fargate to use.")
            .defaultValue("1.4.0")
            .propertyGroup(InstancePropertyGroup.COMMON).build();
    UserDefinedInstanceProperty TASK_RUNNER_LAMBDA_MEMORY_IN_MB = Index.propertyBuilder("sleeper.task.runner.memory.mb")
            .description("The amount of memory in MB for the lambda that creates ECS tasks to execute compaction and ingest jobs.")
            .defaultValue("1024")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.task.runner.timeout.seconds")
            .description("The timeout in seconds for the lambda that creates ECS tasks to execute compaction jobs and ingest jobs.\n" +
                    "This must be >0 and <= 900.")
            .defaultValue("900")
            .validationPredicate(SleeperPropertyValueUtils::isValidLambdaTimeout)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty FORCE_RELOAD_PROPERTIES = Index.propertyBuilder("sleeper.properties.force.reload")
            .description("If true, properties will be reloaded every time a long running job is started or a lambda is run. " +
                    "This will mainly be used in test scenarios to ensure properties are up to date.")
            .defaultValue("false")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    UserDefinedInstanceProperty ECR_REPOSITORY_PREFIX = Index.propertyBuilder("sleeper.ecr.repository.prefix")
            .description("If set, this property will be used as a prefix for the names of ECR repositories. " +
                    "If unset, then the instance ID will be used to determine the names instead.\n" +
                    "Note: This is only used by the deployment scripts to upload Docker images, not the CDK. " +
                    "We may add the ability to use this in the CDK in the future.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .editable(false).build();
    UserDefinedInstanceProperty ECS_SECURITY_GROUPS = Index.propertyBuilder("sleeper.ecs.security.groups")
            .description("A comma-separated list of up to 5 security group IDs to be used when running ECS tasks.")
            .validationPredicate(value -> SleeperPropertyValueUtils.isListWithMaxSize(value, 5))
            .runCdkDeployWhenChanged(true)
            .propertyGroup(InstancePropertyGroup.COMMON).build();
    UserDefinedInstanceProperty DEFAULT_LAMBDA_CONCURRENCY_RESERVED = Index.propertyBuilder("sleeper.default.lambda.concurrency.reserved")
            .description("Default value for the reserved concurrency for each lambda within the Sleeper instance. " +
                    "By default no concurrency is reserved for the lambdas. Each lambda also has its own property " +
                    "that overrides the value found here.\n" +
                    "See reserved concurrency overview at: https://docs.aws.amazon.com/lambda/latest/dg/configuration-concurrency.html")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveIntegerOrNull)
            .defaultValue(null)
            .propertyGroup(InstancePropertyGroup.COMMON).build();
    UserDefinedInstanceProperty DEFAULT_LAMBDA_CONCURRENCY_MAXIMUM = Index.propertyBuilder("sleeper.default.lambda.concurrency.max")
            .description("Default value for the maximum concurrency for each lambda within the Sleeper instance. " +
                    "By default the maximum concurrency is set to 10, which is enough for 10 online tables. " +
                    "If there are more online tables, this number may need to be increased. Each lambda also has its own property that " +
                    "overrides the value found here.\n" +
                    "See maximum concurrency overview at: https://aws.amazon.com/blogs/compute/introducing-maximum-concurrency-of-aws-lambda-functions-when-using-amazon-sqs-as-an-event-source/")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveIntegerOrNull)
            .defaultValue("10")
            .propertyGroup(InstancePropertyGroup.COMMON).build();

    UserDefinedInstanceProperty TRANSACTION_DELETION_LAMBDA_TIMEOUT = Index.propertyBuilder("sleeper.default.deletion.lambda.timeout.minutes")
            .description("The maximum timeout for the trnasaction deletion lambda within minutes")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveIntegerLtEq15)
            .defaultValue("15")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();

    static List<UserDefinedInstanceProperty> getAll() {
        return Index.INSTANCE.getAll();
    }

    /**
     * An index of property definitions in this file.
     */
    class Index {
        private Index() {
        }

        private static final SleeperPropertyIndex<UserDefinedInstanceProperty> INSTANCE = new SleeperPropertyIndex<>();

        private static UserDefinedInstancePropertyImpl.Builder propertyBuilder(String propertyName) {
            return UserDefinedInstancePropertyImpl.named(propertyName)
                    .addToIndex(INSTANCE::add);
        }
    }
}
