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
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.services.cloudwatch.ComparisonOperator;
import software.amazon.awscdk.services.cloudwatch.CreateAlarmOptions;
import software.amazon.awscdk.services.cloudwatch.MetricOptions;
import software.amazon.awscdk.services.cloudwatch.TreatMissingData;
import software.amazon.awscdk.services.cloudwatch.actions.SnsAction;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sns.ITopic;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.Utils;
import sleeper.cdk.jars.BuiltJar;
import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.jars.LambdaCode;
import sleeper.cdk.stack.IngestStatusStoreResources;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.SystemDefinedInstanceProperty;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static sleeper.cdk.stack.IngestStack.addIngestSourceBucketReferences;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.LOG_RETENTION_IN_DAYS;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;

public class CommonEmrBulkImportHelper {

    private final Construct scope;
    private final String shortId;
    private final InstanceProperties instanceProperties;
    private final IngestStatusStoreResources statusStoreResources;

    public CommonEmrBulkImportHelper(Construct scope, String shortId,
                                     InstanceProperties instanceProperties,
                                     IngestStatusStoreResources ingestStatusStoreResources) {
        this.scope = scope;
        this.shortId = shortId;
        this.instanceProperties = instanceProperties;
        this.statusStoreResources = ingestStatusStoreResources;
    }

    // Queue for messages to trigger jobs - note that each concrete substack
    // will have its own queue. The shortId is used to ensure the names of
    // the queues are different.
    public Queue createJobQueue(SystemDefinedInstanceProperty jobQueueUrl, ITopic errorsTopic) {
        String instanceId = instanceProperties.get(ID);
        Queue queueForDLs = Queue.Builder
                .create(scope, "BulkImport" + shortId + "JobDeadLetterQueue")
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
                .createAlarm(scope, "BulkImport" + shortId + "UndeliveredJobsAlarm", CreateAlarmOptions.builder()
                        .alarmDescription("Alarms if there are any messages that have failed validation or failed to start a " + shortId + " EMR Spark job")
                        .evaluationPeriods(1)
                        .comparisonOperator(ComparisonOperator.GREATER_THAN_THRESHOLD)
                        .threshold(0)
                        .datapointsToAlarm(1)
                        .treatMissingData(TreatMissingData.IGNORE)
                        .build())
                .addAlarmAction(new SnsAction(errorsTopic));

        Queue emrBulkImportJobQueue = Queue.Builder
                .create(scope, "BulkImport" + shortId + "JobQueue")
                .deadLetterQueue(deadLetterQueue)
                .visibilityTimeout(Duration.minutes(3))
                .queueName(instanceId + "-BulkImport" + shortId + "Q")
                .build();

        instanceProperties.set(jobQueueUrl, emrBulkImportJobQueue.getQueueUrl());

        return emrBulkImportJobQueue;
    }

    public IFunction createJobStarterFunction(String bulkImportPlatform, Queue jobQueue, BuiltJars jars,
                                              IBucket importBucket, CommonEmrBulkImportStack commonEmrStack) {
        return createJobStarterFunction(bulkImportPlatform, jobQueue, jars, importBucket,
                List.of(commonEmrStack.getEmrRole(), commonEmrStack.getEc2Role()));
    }

    public IFunction createJobStarterFunction(String bulkImportPlatform, Queue jobQueue, BuiltJars jars,
                                              IBucket importBucket, List<IRole> passRoles) {
        String instanceId = instanceProperties.get(ID);
        Map<String, String> env = Utils.createDefaultEnvironment(instanceProperties);
        env.put("BULK_IMPORT_PLATFORM", bulkImportPlatform);
        IBucket jarsBucket = Bucket.fromBucketName(scope, "CodeBucketEMR", instanceProperties.get(JARS_BUCKET));
        LambdaCode bulkImportStarterJar = jars.lambdaCode(BuiltJar.BULK_IMPORT_STARTER, jarsBucket);

        IBucket configBucket = Bucket.fromBucketName(scope, "ConfigBucket", instanceProperties.get(CONFIG_BUCKET));

        String functionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceId.toLowerCase(Locale.ROOT), shortId, "bulk-import-job-starter"));

        IFunction function = bulkImportStarterJar.buildFunction(scope, "BulkImport" + shortId + "JobStarter", builder -> builder
                .functionName(functionName)
                .description("Function to start " + shortId + " bulk import jobs")
                .memorySize(1024)
                .timeout(Duration.minutes(2))
                .environment(env)
                .runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_11)
                .handler("sleeper.bulkimport.starter.BulkImportStarterLambda")
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS)))
                .events(Lists.newArrayList(SqsEventSource.Builder.create(jobQueue).batchSize(1).build())));

        configBucket.grantRead(function);
        importBucket.grantReadWrite(function);
        addIngestSourceBucketReferences(scope, "IngestBucket", instanceProperties)
                .forEach(ingestBucket -> ingestBucket.grantRead(function));
        statusStoreResources.grantWriteJobEvent(function);

        function.addToRolePolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(Lists.newArrayList("iam:PassRole"))
                .resources(passRoles.stream()
                        .map(IRole::getRoleArn)
                        .collect(Collectors.toUnmodifiableList()))
                .build());

        function.addToRolePolicy(PolicyStatement.Builder.create()
                .sid("CreateCleanupRole")
                .actions(Lists.newArrayList("iam:CreateServiceLinkedRole", "iam:PutRolePolicy"))
                .resources(Lists.newArrayList("arn:aws:iam::*:role/aws-service-role/elasticmapreduce.amazonaws.com*/AWSServiceRoleForEMRCleanup*"))
                .conditions(Map.of("StringLike", Map.of("iam:AWSServiceName",
                        Lists.newArrayList("elasticmapreduce.amazonaws.com",
                                "elasticmapreduce.amazonaws.com.cn"))))
                .build());

        function.addToRolePolicy(PolicyStatement.Builder.create()
                .sid("EmrServerlessStartJobRun")
                .actions(Lists.newArrayList("emr-serverless:StartJobRun"))
                .resources(Lists.newArrayList("*"))
                .build());

        return function;
    }
}
