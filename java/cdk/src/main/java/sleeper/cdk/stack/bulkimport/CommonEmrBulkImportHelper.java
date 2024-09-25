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
package sleeper.cdk.stack.bulkimport;

import com.google.common.collect.Lists;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.services.cloudwatch.IMetric;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.Utils;
import sleeper.cdk.jars.BuiltJar;
import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.jars.LambdaCode;
import sleeper.cdk.stack.CoreStacks;
import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static sleeper.cdk.Utils.createAlarmForDlq;
import static sleeper.cdk.Utils.createLambdaLogGroup;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;

public class CommonEmrBulkImportHelper {

    private final Construct scope;
    private final String platform;
    private final InstanceProperties instanceProperties;
    private final CoreStacks coreStacks;
    private final List<IMetric> errorMetrics;

    public CommonEmrBulkImportHelper(
            Construct scope, String platform, InstanceProperties instanceProperties,
            CoreStacks coreStacks, List<IMetric> errorMetrics) {
        this.scope = scope;
        this.platform = platform;
        this.instanceProperties = instanceProperties;
        this.coreStacks = coreStacks;
        this.errorMetrics = errorMetrics;
        if (platform.length() > 16) {
            throw new IllegalArgumentException("platform must be at most 16 characters to create short enough resource names");
        }
    }

    // Queue for messages to trigger jobs - note that each concrete substack
    // will have its own queue. The shortId is used to ensure the names of
    // the queues are different.
    public Queue createJobQueue(CdkDefinedInstanceProperty jobQueueUrl, CdkDefinedInstanceProperty jobQueueArn, Topic errorsTopic) {
        String instanceId = Utils.cleanInstanceId(instanceProperties);
        Queue queueForDLs = Queue.Builder
                .create(scope, "BulkImport" + platform + "JobDeadLetterQueue")
                .queueName(String.join("-", "sleeper", instanceId, "BulkImport" + platform + "DLQ"))
                .build();
        DeadLetterQueue deadLetterQueue = DeadLetterQueue.builder()
                .maxReceiveCount(1)
                .queue(queueForDLs)
                .build();

        createAlarmForDlq(scope, "BulkImport" + platform + "UndeliveredJobsAlarm",
                "Alarms if there are any messages that have failed validation or failed to start a " + platform + " Spark job",
                queueForDLs, errorsTopic);

        errorMetrics.add(Utils.createErrorMetric("Bulk Import " + platform + " Errors", queueForDLs, instanceProperties));
        Queue emrBulkImportJobQueue = Queue.Builder
                .create(scope, "BulkImport" + platform + "JobQueue")
                .deadLetterQueue(deadLetterQueue)
                .visibilityTimeout(Duration.minutes(3))
                .queueName(String.join("-", "sleeper", instanceId, "BulkImport" + platform + "Q"))
                .build();

        instanceProperties.set(jobQueueUrl, emrBulkImportJobQueue.getQueueUrl());
        instanceProperties.set(jobQueueArn, emrBulkImportJobQueue.getQueueArn());
        emrBulkImportJobQueue.grantSendMessages(coreStacks.getIngestByQueuePolicyForGrants());
        emrBulkImportJobQueue.grantPurge(coreStacks.getPurgeQueuesPolicyForGrants());

        return emrBulkImportJobQueue;
    }

    public IFunction createJobStarterFunction(
            String bulkImportPlatform, Queue jobQueue, BuiltJars jars, IBucket importBucket,
            CommonEmrBulkImportStack commonEmrStack) {
        return createJobStarterFunction(bulkImportPlatform, jobQueue, jars, importBucket,
                List.of(commonEmrStack.getEmrRole(), commonEmrStack.getEc2Role()));
    }

    public IFunction createJobStarterFunction(
            String bulkImportPlatform, Queue jobQueue, BuiltJars jars, IBucket importBucket,
            List<IRole> passRoles) {
        Map<String, String> env = Utils.createDefaultEnvironment(instanceProperties);
        env.put("BULK_IMPORT_PLATFORM", bulkImportPlatform);
        IBucket jarsBucket = Bucket.fromBucketName(scope, "CodeBucketEMR", instanceProperties.get(JARS_BUCKET));
        LambdaCode bulkImportStarterJar = jars.lambdaCode(BuiltJar.BULK_IMPORT_STARTER, jarsBucket);

        String functionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "bulk-import", platform, "start");

        IFunction function = bulkImportStarterJar.buildFunction(scope, "BulkImport" + platform + "JobStarter", builder -> builder
                .functionName(functionName)
                .description("Function to start " + platform + " bulk import jobs")
                .memorySize(1024)
                .timeout(Duration.minutes(2))
                .environment(env)
                .runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_11)
                .handler("sleeper.bulkimport.starter.BulkImportStarterLambda")
                .logGroup(createLambdaLogGroup(scope, "BulkImport" + platform + "JobStarterLogGroup", functionName, instanceProperties))
                .events(Lists.newArrayList(SqsEventSource.Builder.create(jobQueue).batchSize(1).build())));

        coreStacks.grantValidateBulkImport(function.getRole());
        importBucket.grantReadWrite(function);

        function.addToRolePolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(Lists.newArrayList("iam:PassRole"))
                .resources(passRoles.stream()
                        .map(IRole::getRoleArn)
                        .collect(Collectors.toUnmodifiableList()))
                .build());

        return function;
    }
}
