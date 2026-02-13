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
package sleeper.cdk.stack.bulkimport;

import com.google.common.collect.Lists;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.bulkimport.core.configuration.BulkImportPlatform;
import sleeper.cdk.artefacts.SleeperArtefacts;
import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.cdk.stack.SleeperCoreStacks;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.EnvironmentUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static sleeper.core.properties.instance.BulkImportProperty.BULK_IMPORT_STARTER_LAMBDA_MEMORY;

public class CommonEmrBulkImportHelper {

    private final Construct scope;
    private final BulkImportPlatform platform;
    private final InstanceProperties instanceProperties;
    private final SleeperCoreStacks coreStacks;

    public CommonEmrBulkImportHelper(
            Construct scope, BulkImportPlatform platform, InstanceProperties instanceProperties,
            SleeperCoreStacks coreStacks) {
        this.scope = scope;
        this.platform = platform;
        this.instanceProperties = instanceProperties;
        this.coreStacks = coreStacks;
        if (platform.toString().length() > 16) {
            throw new IllegalArgumentException("platform must be at most 16 characters to create short enough resource names");
        }
    }

    // Queue for messages to trigger jobs - note that each concrete substack
    // will have its own queue. The shortId is used to ensure the names of
    // the queues are different.
    public Queue createJobQueue(CdkDefinedInstanceProperty jobQueueUrl, CdkDefinedInstanceProperty jobQueueArn) {
        String instanceId = Utils.cleanInstanceId(instanceProperties);
        Queue queueForDLs = Queue.Builder
                .create(scope, "BulkImport" + platform + "JobDeadLetterQueue")
                .queueName(String.join("-", "sleeper", instanceId, "BulkImport" + platform + "DLQ"))
                .build();
        DeadLetterQueue deadLetterQueue = DeadLetterQueue.builder()
                .maxReceiveCount(1)
                .queue(queueForDLs)
                .build();

        coreStacks.alarmOnDeadLetters(scope, "BulkImport" + platform + "UndeliveredJobsAlarm", "starting a " + platform + " Spark job", queueForDLs);

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
            Queue jobQueue, SleeperArtefacts artefacts, IBucket importBucket, LogGroupRef logGroupRef, CommonEmrBulkImportStack commonEmrStack) {
        SleeperLambdaCode lambdaCode = SleeperLambdaCode.atScope(scope, instanceProperties, artefacts);
        return createJobStarterFunction(jobQueue, lambdaCode, importBucket, logGroupRef,
                List.of(commonEmrStack.getEmrRole(), commonEmrStack.getEc2Role()));
    }

    public IFunction createJobStarterFunction(
            Queue jobQueue, SleeperLambdaCode lambdaCode, IBucket importBucket, LogGroupRef logGroupRef,
            List<IRole> passRoles) {
        Map<String, String> env = EnvironmentUtils.createDefaultEnvironment(instanceProperties);
        env.put("BULK_IMPORT_PLATFORM", platform.toString());

        String functionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "bulk-import", platform.toString(), "start");

        IFunction function = lambdaCode.buildFunction(scope, LambdaHandler.BULK_IMPORT_STARTER, "BulkImport" + platform + "JobStarter", builder -> builder
                .functionName(functionName)
                .description("Function to start " + platform + " bulk import jobs")
                .memorySize(instanceProperties.getInt(BULK_IMPORT_STARTER_LAMBDA_MEMORY))
                .timeout(Duration.minutes(2))
                .environment(env)
                .logGroup(coreStacks.getLogGroup(logGroupRef))
                .events(Lists.newArrayList(SqsEventSource.Builder.create(jobQueue)
                        .batchSize(1)
                        .maxConcurrency(2)
                        .build())));

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
