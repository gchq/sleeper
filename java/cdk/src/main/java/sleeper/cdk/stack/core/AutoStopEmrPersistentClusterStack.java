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
package sleeper.cdk.stack.core;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.awscdk.CustomResource;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.customresources.Provider;
import software.amazon.awscdk.services.emr.CfnCluster;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.jars.LambdaCode;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.EnvironmentUtils;

import java.util.List;
import java.util.Map;

/**
 * Stops EMR persistent cluster for the CloudFormation stack.
 */
public class AutoStopEmrPersistentClusterStack extends NestedStack {

    private IFunction lambda;
    private Provider provider;

    public AutoStopEmrPersistentClusterStack(Construct scope, String id, InstanceProperties instanceProperties, BuiltJars jars,
            LoggingStack loggingStack) {
        super(scope, id);
        createLambda(instanceProperties, jars, loggingStack);
    }

    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    private void createLambda(InstanceProperties instanceProperties, BuiltJars jars, LoggingStack loggingStack) {

        // Jars bucket
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", jars.bucketName());
        LambdaCode lambdaCode = jars.lambdaCode(jarsBucket);

        String functionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "auto-stop-emr-persistent-cluster");

        lambda = lambdaCode.buildFunction(this, LambdaHandler.AUTO_STOP_EMR_PERSISTENT_CLUSTER, "Lambda", builder -> builder
                .functionName(functionName)
                .memorySize(2048)
                .environment(EnvironmentUtils.createDefaultEnvironmentNoConfigBucket(instanceProperties))
                .description("Lambda for auto-stopping EMR Persistent cluster")
                .logGroup(loggingStack.getLogGroup(LogGroupRef.AUTO_STOP_EMR_PERSISTENT_CLUSTER))
                .timeout(Duration.minutes(15)));

        // Grant this function permission to emrserverless actions
        lambda.getRole().addToPrincipalPolicy(PolicyStatement.Builder
                .create()
                .resources(List.of("*"))
                .actions(List.of("elasticmapreduce:DescribeCluster", "elasticmapreduce:TerminateJobFlows"))
                .build());

        provider = Provider.Builder.create(this, "Provider")
                .onEventHandler(lambda)
                .logGroup(loggingStack.getLogGroup(LogGroupRef.AUTO_STOP_EMR_PERSISTENT_CLUSTER_PROVIDER))
                .build();

        Utils.addStackTagIfSet(this, instanceProperties);
    }

    /**
     * Add a custom resource to stop EMR persistent clusters.
     *
     * @param scope   the stack to add the custom resource to
     * @param cluster the EMR persistent cluster
     */
    public void addAutoStopEmrPersistentCluster(Construct scope, CfnCluster cluster) {

        String id = cluster.getNode().getId() + "-Autostop";

        CustomResource customResource = CustomResource.Builder.create(scope, id)
                .resourceType("Custom::AutoStopEmrPersistentCluster")
                .properties(Map.of("clusterId", cluster.getClusterRef().getClusterId()))
                .serviceToken(provider.getServiceToken())
                .build();

        customResource.getNode().addDependency(cluster);

    }

}
