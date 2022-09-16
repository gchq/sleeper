/*
 * Copyright 2022 Crown Copyright
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import sleeper.cdk.Utils;
import sleeper.cdk.stack.StateStoreStack;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.SystemDefinedInstanceProperty;
import sleeper.configuration.properties.UserDefinedInstanceProperty;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.S3Code;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sns.ITopic;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_JOB_QUEUE_URL;

/**
 * An {@link EmrBulkImportStack} creates an SQS queue that bulk import jobs can
 * be sent to. A message arriving on this queue triggers a lambda. That lambda
 * creates an EMR cluster that executes the bulk import job and then terminates.
 */
public class EmrBulkImportStack extends AbstractEmrBulkImportStack {
    protected Function bulkImportJobStarter;

    public EmrBulkImportStack(
            Construct scope,
            String id,
            List<IBucket> dataBuckets,
            List<StateStoreStack> stateStoreStacks,
            InstanceProperties instanceProperties,
            ITopic errorsTopic) {
        super(scope, id, "NonPersistentEMR", "NonPersistentEMR",
                BULK_IMPORT_EMR_JOB_QUEUE_URL, dataBuckets, stateStoreStacks, instanceProperties, errorsTopic);

        // Create function to pull messages off queue and start jobs
        createBulkImportJobStarterFunction(shortId, bulkImportPlatform, bulkImportJobQueue);
    }

    private void createBulkImportJobStarterFunction(String shortId, String bulkImportPlatform, Queue jobQueue) {
        Map<String, String> env = Utils.createDefaultEnvironment(instanceProperties);
        env.put("BULK_IMPORT_PLATFORM", bulkImportPlatform);
        S3Code code = Code.fromBucket(Bucket.fromBucketName(this, "CodeBucketEMR", instanceProperties.get(UserDefinedInstanceProperty.JARS_BUCKET)),
                "bulk-import-starter-" + instanceProperties.get(UserDefinedInstanceProperty.VERSION) + ".jar");

        IBucket configBucket = Bucket.fromBucketName(this, "ConfigBucket", instanceProperties.get(SystemDefinedInstanceProperty.CONFIG_BUCKET));

        String functionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceId.toLowerCase(Locale.ROOT), shortId, "-bulk-import-job-starter"));

        bulkImportJobStarter = Function.Builder.create(this, "BulkImport" + shortId + "JobStarter")
                .code(code)
                .functionName(functionName)
                .description("Function to start " + shortId + " bulk import jobs")
                .memorySize(1024)
                .timeout(Duration.seconds(10))
                .environment(env)
                .runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_8)
                .handler("sleeper.bulkimport.starter.BulkImportStarter")
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(UserDefinedInstanceProperty.LOG_RETENTION_IN_DAYS)))
                .events(Lists.newArrayList(new SqsEventSource(jobQueue)))
                .build();

        configBucket.grantReadWrite(bulkImportJobStarter);
        if (importBucket != null) {
            importBucket.grantRead(bulkImportJobStarter);
        }
        if (ingestBucket != null) {
            ingestBucket.grantRead(bulkImportJobStarter);
        }

        Map<String, Map<String, String>> conditions = new HashMap<>();
        Map<String, String> tagKeyCondition = new HashMap<>();

        instanceProperties.getTags().entrySet().stream().forEach(entry -> {
            tagKeyCondition.put("elasticmapreduce:RequestTag/" + entry.getKey(), entry.getValue());
        });

        conditions.put("StringEquals", tagKeyCondition);

        bulkImportJobStarter.addToRolePolicy(PolicyStatement.Builder.create()
                .actions(Lists.newArrayList("elasticmapreduce:RunJobFlow"))
                .effect(Effect.ALLOW)
                .resources(Lists.newArrayList("*"))
                .conditions(conditions)
                .build());

        String arnPrefix = "arn:aws:iam::" + instanceProperties.get(UserDefinedInstanceProperty.ACCOUNT) + ":role/";

        bulkImportJobStarter.addToRolePolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(Lists.newArrayList("iam:PassRole"))
                .resources(Lists.newArrayList(
                        arnPrefix + instanceProperties.get(SystemDefinedInstanceProperty.BULK_IMPORT_EMR_CLUSTER_ROLE_NAME),
                        arnPrefix + instanceProperties.get(SystemDefinedInstanceProperty.BULK_IMPORT_EMR_EC2_ROLE_NAME)
                ))
                .build());

        bulkImportJobStarter.addToRolePolicy(PolicyStatement.Builder.create()
                .sid("CreateCleanupRole")
                .actions(Lists.newArrayList("iam:CreateServiceLinkedRole", "iam:PutRolePolicy"))
                .resources(Lists.newArrayList("arn:aws:iam::*:role/aws-service-role/elasticmapreduce.amazonaws.com*/AWSServiceRoleForEMRCleanup*"))
                .conditions(ImmutableMap.of("StringLike", ImmutableMap.of("iam:AWSServiceName",
                        Lists.newArrayList("elasticmapreduce.amazonaws.com",
                                "elasticmapreduce.amazonaws.com.cn"))))
                .build());

        Utils.addStackTagIfSet(this, instanceProperties);
    }
}
