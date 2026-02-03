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

package sleeper.core.properties.instance;

import sleeper.core.properties.SleeperPropertyIndex;
import sleeper.core.properties.model.LambdaDeployType;
import sleeper.core.properties.model.OptionalStack;
import sleeper.core.properties.model.SleeperArtefactsLocation;
import sleeper.core.properties.model.SleeperPropertyValueUtils;

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
    UserDefinedInstanceProperty ARTEFACTS_DEPLOYMENT_ID = Index.propertyBuilder("sleeper.artefacts.deployment")
            .description("The ID of the artefacts deployment to use to deploy the Sleeper instance. By default " +
                    "we assume an artefacts deployment with the same ID as the Sleeper instance. This property is " +
                    "used to compute the default values of `sleeper.jars.bucket` and `sleeper.ecr.repository.prefix`.")
            .defaultProperty(ID)
            .validationPredicate(Objects::nonNull)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty JARS_BUCKET = Index.propertyBuilder("sleeper.jars.bucket")
            .description("The S3 bucket containing the jar files of the Sleeper components. If unset, a default name " +
                    "is computed from `sleeper.artefacts.deployment` if it is set, or `sleeper.id` if it is not.")
            .defaultProperty(ARTEFACTS_DEPLOYMENT_ID, SleeperArtefactsLocation::getDefaultJarsBucketName)
            .validationPredicate(Objects::nonNull)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty ECR_REPOSITORY_PREFIX = Index.propertyBuilder("sleeper.ecr.repository.prefix")
            .description("If set, this property will be used as a prefix for the names of ECR repositories. " +
                    "If unset, a default prefix is computed from `sleeper.artefacts.deployment` if it is set, or " +
                    "`sleeper.id` if it is not.\n" +
                    "ECR repository names are generated in the format `<prefix>/<image name>`.")
            .defaultProperty(ARTEFACTS_DEPLOYMENT_ID, SleeperArtefactsLocation::getDefaultEcrRepositoryPrefix)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty USER_JARS = Index.propertyBuilder("sleeper.userjars")
            .description("A comma-separated list of the jars containing application specific iterator code. " +
                    "These jars are assumed to be in the bucket given by `sleeper.jars.bucket`. For example, if that " +
                    "bucket contains two iterator jars called iterator1.jar and iterator2.jar then the " +
                    "property should be 'sleeper.userjars=iterator1.jar,iterator2.jar'.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .includedInBasicTemplate(true).build();
    UserDefinedInstanceProperty TAGS = Index.propertyBuilder("sleeper.tags")
            .description("A list of tags that will automatically be applied to all the resources in this deployment " +
                    "of Sleeper. The list should be in the form \"key1,value1,key2,value2,key3,value3,...\".\n" +
                    "For example if you want to add tags of \"user=some-user\" and \"project-name=sleeper-test\", " +
                    "then the list should be \"user,some-user,project-name,sleeper-test\".\n" +
                    "Preferably, tags should be specified in a separate file called tags.properties.\n" +
                    "See https://github.com/gchq/sleeper/blob/develop/docs/deployment/instance-configuration.md for further details.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .validationPredicate(SleeperPropertyValueUtils::isListInKeyValueFormat)
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
    UserDefinedInstanceProperty RETAIN_LOGS_AFTER_DESTROY = Index.propertyBuilder("sleeper.retain.logs.after.destroy")
            .description("Whether to keep the sleeper log groups when the instance is destroyed.")
            .defaultValue("true")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true)
            .includedInBasicTemplate(true).build();
    UserDefinedInstanceProperty DEFAULT_RETAIN_TABLE_AFTER_REMOVAL = Index.propertyBuilder("sleeper.default.table.retain.after.removal")
            .description("This property is used when applying an instance configuration and a table has been removed.\n" +
                    "If this is true (default), removing the table from the configuration will just take the table offline.\n" +
                    "If this is false, it will delete all data associated with the table when the table is removed.\n" +
                    "Be aware that if a table is renamed in the configuration, the CDK will see it as a delete of the old " +
                    "table name and a create of the new table name. If this is set to false when that happens it will remove the table's data.\n" +
                    "This property isn't currently in use but will be in https://github.com/gchq/sleeper/issues/5870.")
            .defaultValue("true")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .includedInBasicTemplate(true).build();
    UserDefinedInstanceProperty DEFAULT_TABLE_REUSE_EXISTING = Index.propertyBuilder("sleeper.default.table.reuse.existing")
            .description("This property is used when applying an instance configuration and a table has been added.\n" +
                    "By default, or if this property is false, when a table is added to an instance configuration it's created " +
                    "in the instance. If it already exists the update will fail.\n" +
                    "If this property is true, the existing table will be reused and imported as part of the instance configuration. " +
                    "If it doesn't exist the update will fail.")
            .defaultValue("false")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.COMMON)
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
                    "There are two types of Lambda deployments, jar and container.\n" +
                    "If the size of the jar file is too large, it will always be deployed as a container.\n" +
                    "Valid values: " + SleeperPropertyValueUtils.describeEnumValuesInLowerCase(LambdaDeployType.class))
            .defaultValue(LambdaDeployType.JAR.toString().toLowerCase(Locale.ROOT))
            .validationPredicate(LambdaDeployType::isValid)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .runCdkDeployWhenChanged(true)
            .build();
    UserDefinedInstanceProperty ENDPOINT_URL = Index.propertyBuilder("sleeper.endpoint.url")
            .description("The AWS endpoint URL. This should only be set for a non-standard service endpoint. Usually " +
                    "this is used to set the URL to LocalStack for a locally deployed instance.")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .editable(false).build();
    UserDefinedInstanceProperty VPC_ID = Index.propertyBuilder("sleeper.vpc")
            .description("The id of the VPC to deploy to. This property may be passed as an argument during " +
                    "deployment. If using the Sleeper CDK app, you can set the context variable \"vpc\". If using " +
                    "your own CDK app, you can set this in SleeperInstanceProps under networking.")
            .validationPredicate(Objects::nonNull)
            .propertyGroup(InstancePropertyGroup.COMMON)
            .editable(false).build();
    UserDefinedInstanceProperty VPC_ENDPOINT_CHECK = Index.propertyBuilder("sleeper.vpc.endpoint.check")
            .description("Whether to check that the VPC that the instance is deployed to has an S3 endpoint. " +
                    "If there is no S3 endpoint then the NAT costs can be very significant.")
            .defaultValue("true")
            .propertyGroup(InstancePropertyGroup.COMMON)
            .build();
    UserDefinedInstanceProperty SUBNETS = Index.propertyBuilder("sleeper.subnets")
            .description("A comma separated list of subnets to deploy to. ECS tasks will be run across multiple " +
                    "subnets. EMR clusters will be deployed in a subnet chosen when the cluster is created. " +
                    "This property may be passed as an argument during deployment. If using the Sleeper CDK app, you " +
                    "can set the context variable \"subnets\". If using your own CDK app, you can set this in " +
                    "SleeperInstanceProps under networking.")
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
    UserDefinedInstanceProperty ECS_SECURITY_GROUPS = Index.propertyBuilder("sleeper.ecs.security.groups")
            .description("A comma-separated list of up to 5 security group IDs to be used when running ECS tasks.")
            .validationPredicate(value -> SleeperPropertyValueUtils.isListWithMaxSize(value, 5))
            .runCdkDeployWhenChanged(true)
            .propertyGroup(InstancePropertyGroup.COMMON).build();
    UserDefinedInstanceProperty DEFAULT_LAMBDA_CONCURRENCY_RESERVED = Index.propertyBuilder("sleeper.default.lambda.concurrency.reserved")
            .description("Default value for the reserved concurrency for each lambda in the Sleeper instance " +
                    "that scales according to the number of Sleeper tables.\n" +
                    "The state store committer lambda is an exception to this, as it has reserved concurrency by " +
                    "default. This is set in the property sleeper.statestore.committer.concurrency.reserved. " +
                    "Other lambdas are present that do not scale by the number of Sleeper tables, and are not " +
                    "set from this property.\n" +
                    "By default no concurrency is reserved for the lambdas. Each lambda also has its own property " +
                    "that overrides the value found here.\n" +
                    "See reserved concurrency overview at: https://docs.aws.amazon.com/lambda/latest/dg/configuration-concurrency.html")
            .validationPredicate(SleeperPropertyValueUtils::isValidSqsLambdaMaximumConcurrency)
            .defaultValue(null)
            .propertyGroup(InstancePropertyGroup.COMMON).build();
    UserDefinedInstanceProperty DEFAULT_LAMBDA_CONCURRENCY_MAXIMUM = Index.propertyBuilder("sleeper.default.lambda.concurrency.max")
            .description("Default value for the maximum concurrency for each lambda in the Sleeper instance that " +
                    "scales according to the number of Sleeper tables.\n" +
                    "Other lambdas are present that do not scale by the number of Sleeper tables, and are not " +
                    "set from this property.\n" +
                    "By default the maximum concurrency is set to 10, which is enough for 10 online tables. " +
                    "If there are more online tables, this number may need to be increased. Each lambda also has its own property that " +
                    "overrides the value found here.\n" +
                    "See maximum concurrency overview at: https://aws.amazon.com/blogs/compute/introducing-maximum-concurrency-of-aws-lambda-functions-when-using-amazon-sqs-as-an-event-source/")
            .validationPredicate(SleeperPropertyValueUtils::isValidSqsLambdaMaximumConcurrency)
            .defaultValue("10")
            .propertyGroup(InstancePropertyGroup.COMMON).build();

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
