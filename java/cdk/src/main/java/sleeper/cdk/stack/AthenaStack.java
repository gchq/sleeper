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
package sleeper.cdk.stack;

import com.google.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.services.athena.CfnDataCatalog;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.Policy;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.kms.IKey;
import software.amazon.awscdk.services.kms.Key;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.s3.BlockPublicAccess;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketEncryption;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.s3.LifecycleRule;
import software.constructs.Construct;

import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.jars.LambdaCode;
import sleeper.cdk.stack.core.CoreStacks;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.AutoDeleteS3Objects;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.Map;
import java.util.Objects;

import static sleeper.core.properties.instance.AthenaProperty.ATHENA_COMPOSITE_HANDLER_CLASSES;
import static sleeper.core.properties.instance.AthenaProperty.ATHENA_COMPOSITE_HANDLER_MEMORY;
import static sleeper.core.properties.instance.AthenaProperty.ATHENA_COMPOSITE_HANDLER_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.AthenaProperty.ATHENA_SPILL_MASTER_KEY_ARN;
import static sleeper.core.properties.instance.AthenaProperty.SPILL_BUCKET_AGE_OFF_IN_DAYS;
import static sleeper.core.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.core.properties.instance.CommonProperty.REGION;

@SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
public class AthenaStack extends NestedStack {
    public AthenaStack(
            Construct scope, String id, InstanceProperties instanceProperties, BuiltJars jars, CoreStacks coreStacks) {
        super(scope, id);

        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", jars.bucketName());
        LambdaCode lambdaCode = jars.lambdaCode(jarsBucket);

        String bucketName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "spill-bucket");

        Bucket spillBucket = Bucket.Builder.create(this, "SpillBucket")
                .bucketName(bucketName)
                .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
                .encryption(BucketEncryption.S3_MANAGED)
                .lifecycleRules(Lists.newArrayList(LifecycleRule.builder()
                        .expiration(Duration.days(instanceProperties.getInt(SPILL_BUCKET_AGE_OFF_IN_DAYS)))
                        .build()))
                .removalPolicy(RemovalPolicy.DESTROY)
                .build();

        AutoDeleteS3Objects.autoDeleteForBucket(this, instanceProperties, lambdaCode, spillBucket, bucketName,
                coreStacks.getLogGroup(LogGroupRef.SPILL_BUCKET_AUTODELETE), coreStacks.getLogGroup(LogGroupRef.SPILL_BUCKET_AUTODELETE_PROVIDER));

        IKey spillMasterKey = createSpillMasterKey(this, instanceProperties);

        createConnector("sleeper.athena.composite.SimpleCompositeHandler",
                LambdaHandler.ATHENA_SIMPLE_COMPOSITE, LogGroupRef.SIMPLE_ATHENA_HANDLER,
                instanceProperties, coreStacks, lambdaCode, jarsBucket, spillBucket, spillMasterKey);
        createConnector("sleeper.athena.composite.IteratorApplyingCompositeHandler",
                LambdaHandler.ATHENA_ITERATORS_COMPOSITE, LogGroupRef.ITERATOR_APPLYING_ATHENA_HANDLER,
                instanceProperties, coreStacks, lambdaCode, jarsBucket, spillBucket, spillMasterKey);

        Utils.addStackTagIfSet(this, instanceProperties);
    }

    private static IKey createSpillMasterKey(Construct scope, InstanceProperties instanceProperties) {
        String spillKeyArn = instanceProperties.get(ATHENA_SPILL_MASTER_KEY_ARN);
        if (spillKeyArn == null) {
            return Key.Builder.create(scope, "SpillMasterKey")
                    .description("Key used to encrypt data in the Athena spill bucket for Sleeper.")
                    .enableKeyRotation(true)
                    .removalPolicy(RemovalPolicy.DESTROY)
                    .pendingWindow(Duration.days(7))
                    .build();
        } else {
            return Key.fromKeyArn(scope, "SpillMasterKey", spillKeyArn);
        }
    }

    private void createConnector(
            String className, LambdaHandler handler, LogGroupRef logGroupRef, InstanceProperties instanceProperties, CoreStacks coreStacks,
            LambdaCode lambdaCode, IBucket jarsBucket, IBucket spillBucket, IKey spillMasterKey) {
        if (!instanceProperties.getList(ATHENA_COMPOSITE_HANDLER_CLASSES).contains(className)) {
            return;
        }
        String instanceId = Utils.cleanInstanceId(instanceProperties);
        String simpleClassName = getSimpleClassName(className);

        String functionName = String.join("-", "sleeper", instanceId, simpleClassName, "athena-handler");

        Map<String, String> env = Utils.createDefaultEnvironment(instanceProperties);
        env.put("spill_bucket", spillBucket.getBucketName());
        env.put("kms_key_id", spillMasterKey.getKeyId());

        IFunction athenaCompositeHandler = lambdaCode.buildFunction(this, handler, simpleClassName + "AthenaCompositeHandler", builder -> builder
                .functionName(functionName)
                .memorySize(instanceProperties.getInt(ATHENA_COMPOSITE_HANDLER_MEMORY))
                .timeout(Duration.seconds(instanceProperties.getInt(ATHENA_COMPOSITE_HANDLER_TIMEOUT_IN_SECONDS)))
                .logGroup(coreStacks.getLogGroup(logGroupRef))
                .environment(env));

        CfnDataCatalog.Builder.create(this, simpleClassName + "AthenaDataCatalog")
                .name(instanceId + simpleClassName + "SleeperConnector")
                .description("Athena Connector for Sleeper")
                .type("LAMBDA")
                .parameters(Map.of("function", athenaCompositeHandler.getFunctionArn()))
                .build();

        jarsBucket.grantRead(athenaCompositeHandler);

        coreStacks.grantReadTablesAndData(athenaCompositeHandler);
        spillBucket.grantReadWrite(athenaCompositeHandler);
        spillMasterKey.grant(athenaCompositeHandler, "kms:GenerateDataKey", "kms:DescribeKey");

        IRole role = Objects.requireNonNull(athenaCompositeHandler.getRole());

        // Required for when spill bucket changes
        role.attachInlinePolicy(Policy.Builder.create(this, "ListAllBucketsPolicy")
                .statements(Lists.newArrayList(PolicyStatement.Builder.create()
                        .resources(Lists.newArrayList("*"))
                        .actions(Lists.newArrayList("S3:ListAllMyBuckets"))
                        .build()))
                .build());

        // Required for when creating a encryption data key
        role.attachInlinePolicy(Policy.Builder.create(this, "KeyGenerationPolicy")
                .statements(Lists.newArrayList(PolicyStatement.Builder.create()
                        .resources(Lists.newArrayList("*"))
                        .actions(Lists.newArrayList("kms:GenerateRandom"))
                        .build()))
                .build());

        // Allow our function to get the status of the query. Allowed to query all workgroups within this account
        // and region
        role.attachInlinePolicy(Policy.Builder.create(this, "GetAthenaQueryStatusPolicy")
                .statements(Lists.newArrayList(PolicyStatement.Builder.create()
                        .resources(Lists.newArrayList("arn:aws:athena:" + instanceProperties.get(REGION) + ":"
                                + instanceProperties.get(ACCOUNT) + ":workgroup/*"))
                        .actions(Lists.newArrayList("athena:getQueryExecution"))
                        .build()))
                .build());
    }

    private static String getSimpleClassName(String className) {
        String simpleClassName = className.substring(className.lastIndexOf(".") + 1);
        if (simpleClassName.endsWith("CompositeHandler")) {
            return simpleClassName.substring(0, simpleClassName.indexOf("CompositeHandler"));
        }
        return simpleClassName;
    }
}
