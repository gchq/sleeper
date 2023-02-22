/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.cdk.stack.bulkimport;

import com.google.common.collect.Lists;
import software.amazon.awscdk.CfnJson;
import software.amazon.awscdk.CfnJsonProps;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.cloudwatch.ComparisonOperator;
import software.amazon.awscdk.services.cloudwatch.CreateAlarmOptions;
import software.amazon.awscdk.services.cloudwatch.MetricOptions;
import software.amazon.awscdk.services.cloudwatch.TreatMissingData;
import software.amazon.awscdk.services.cloudwatch.actions.SnsAction;
import software.amazon.awscdk.services.emr.CfnSecurityConfiguration;
import software.amazon.awscdk.services.emr.CfnSecurityConfigurationProps;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.S3Code;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sns.ITopic;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.Utils;
import sleeper.cdk.stack.StateStoreStack;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.SystemDefinedInstanceProperty;
import sleeper.configuration.properties.UserDefinedInstanceProperty;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.LOG_RETENTION_IN_DAYS;

public abstract class AbstractEmrBulkImportStack extends NestedStack {
    protected final String id;
    protected final String shortId;
    protected final String bulkImportPlatform;
    protected final SystemDefinedInstanceProperty jobQueueUrl;
    protected final List<IBucket> dataBuckets;
    protected final List<StateStoreStack> stateStoreStacks;
    private final ITopic errorsTopic;
    protected final IBucket ingestBucket;
    protected final IBucket importBucket;
    protected final InstanceProperties instanceProperties;
    protected final String instanceId;
    protected final String account;
    protected final String region;
    protected final String vpc;
    protected final String subnet;
    protected Queue bulkImportJobQueue;
    protected Function bulkImportJobStarter;

    protected AbstractEmrBulkImportStack(
            Construct scope,
            String id,
            String shortId,
            String bulkImportPlatform,
            SystemDefinedInstanceProperty jobQueueUrl,
            IBucket importBucket,
            List<IBucket> dataBuckets,
            List<StateStoreStack> stateStoreStacks,
            InstanceProperties instanceProperties,
            ITopic errorsTopic) {
        super(scope, id);
        this.id = id;
        this.shortId = shortId;
        this.bulkImportPlatform = bulkImportPlatform;
        this.jobQueueUrl = jobQueueUrl;
        this.importBucket = importBucket;
        this.dataBuckets = dataBuckets;
        this.stateStoreStacks = stateStoreStacks;
        this.errorsTopic = errorsTopic;
        this.instanceProperties = instanceProperties;
        this.instanceId = instanceProperties.get(ID);
        this.account = instanceProperties.get(UserDefinedInstanceProperty.ACCOUNT);
        this.region = instanceProperties.get(UserDefinedInstanceProperty.REGION);
        this.subnet = instanceProperties.get(UserDefinedInstanceProperty.SUBNET);
        this.vpc = instanceProperties.get(UserDefinedInstanceProperty.VPC_ID);

        // Ingest bucket
        String ingestBucketName = instanceProperties.get(UserDefinedInstanceProperty.INGEST_SOURCE_BUCKET);
        if (null != ingestBucketName && !ingestBucketName.isEmpty()) {
            this.ingestBucket = Bucket.fromBucketName(this, "IngestBucket", ingestBucketName);
        } else {
            this.ingestBucket = null;
        }
    }

    public void create() {
        // Queue for messages to trigger jobs - note that each concrete substack
        // will have its own queue. The shortId is used to ensure the names of
        // the queues are different.
        bulkImportJobQueue = createQueues(shortId, jobQueueUrl);

        // Create security configuration
        createSecurityConfiguration();

        // Create bulk import job starter function
        createBulkImportJobStarterFunction();

        Utils.addStackTagIfSet(this, instanceProperties);
    }

    private Queue createQueues(String shortId, SystemDefinedInstanceProperty jobQueueUrl) {
        Queue queueForDLs = Queue.Builder
                .create(this, "BulkImport" + shortId + "JobDeadLetterQueue")
                .queueName(String.join("-", "sleeper", instanceId, "BulkImport" + shortId + "DLQ"))
                .build();

        DeadLetterQueue deadLetterQueue = DeadLetterQueue.builder()
                .maxReceiveCount(1)
                .queue(queueForDLs)
                .build();

        queueForDLs.metricApproximateNumberOfMessagesVisible().with(MetricOptions.builder()
                        .period(Duration.seconds(60))
                        .statistic("Sum")
                        .build())
                .createAlarm(this, "BulkImport" + shortId + "UndeliveredJobsAlarm", CreateAlarmOptions.builder()
                        .alarmDescription("Alarms if there are any messages that have failed validation or failed to start a " + shortId + " EMR Spark job")
                        .evaluationPeriods(1)
                        .comparisonOperator(ComparisonOperator.GREATER_THAN_THRESHOLD)
                        .threshold(0)
                        .datapointsToAlarm(1)
                        .treatMissingData(TreatMissingData.IGNORE)
                        .build())
                .addAlarmAction(new SnsAction(errorsTopic));

        Queue emrBulkImportJobQueue = Queue.Builder
                .create(this, "BulkImport" + shortId + "JobQueue")
                .deadLetterQueue(deadLetterQueue)
                .queueName(instanceId + "-BulkImport" + shortId + "Q")
                .build();

        instanceProperties.set(jobQueueUrl, emrBulkImportJobQueue.getQueueUrl());

        return emrBulkImportJobQueue;
    }

    protected void createSecurityConfiguration() {
        if (null == instanceProperties.get(SystemDefinedInstanceProperty.BULK_IMPORT_EMR_SECURITY_CONF_NAME)) {
            // See https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-create-security-configuration.html
            String jsonSecurityConf = "{\n" +
                    "  \"InstanceMetadataServiceConfiguration\" : {\n" +
                    "      \"MinimumInstanceMetadataServiceVersion\": 2,\n" +
                    "      \"HttpPutResponseHopLimit\": 1\n" +
                    "   }\n" +
                    "}";
            CfnJsonProps jsonProps = CfnJsonProps.builder().value(jsonSecurityConf).build();
            CfnJson jsonObject = new CfnJson(this, "EMRSecurityConfigurationJSONObject", jsonProps);
            CfnSecurityConfigurationProps securityConfigurationProps = CfnSecurityConfigurationProps.builder()
                    .name(String.join("-", "sleeper", instanceId, "EMRSecurityConfigurationProps"))
                    .securityConfiguration(jsonObject)
                    .build();
            new CfnSecurityConfiguration(this, "EMRSecurityConfiguration", securityConfigurationProps);
            instanceProperties.set(SystemDefinedInstanceProperty.BULK_IMPORT_EMR_SECURITY_CONF_NAME, securityConfigurationProps.getName());
        }
    }

    public Queue getEmrBulkImportJobQueue() {
        return bulkImportJobQueue;
    }

    protected void createBulkImportJobStarterFunction() {
        Map<String, String> env = Utils.createDefaultEnvironment(instanceProperties);
        env.put("BULK_IMPORT_PLATFORM", bulkImportPlatform);
        S3Code code = Code.fromBucket(Bucket.fromBucketName(this, "CodeBucketEMR", instanceProperties.get(JARS_BUCKET)),
                "bulk-import-starter-" + instanceProperties.get(UserDefinedInstanceProperty.VERSION) + ".jar");

        IBucket configBucket = Bucket.fromBucketName(this, "ConfigBucket", instanceProperties.get(CONFIG_BUCKET));

        String functionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceId.toLowerCase(Locale.ROOT), shortId, "bulk-import-job-starter"));

        bulkImportJobStarter = Function.Builder.create(this, "BulkImport" + shortId + "JobStarter")
                .code(code)
                .functionName(functionName)
                .description("Function to start " + shortId + " bulk import jobs")
                .memorySize(1024)
                .timeout(Duration.seconds(20))
                .environment(env)
                .runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_11)
                .handler("sleeper.bulkimport.starter.BulkImportStarter")
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS)))
                .events(Lists.newArrayList(new SqsEventSource(bulkImportJobQueue)))
                .build();

        configBucket.grantRead(bulkImportJobStarter);
        importBucket.grantReadWrite(bulkImportJobStarter);
        if (ingestBucket != null) {
            ingestBucket.grantRead(bulkImportJobStarter);
        }
    }
}
