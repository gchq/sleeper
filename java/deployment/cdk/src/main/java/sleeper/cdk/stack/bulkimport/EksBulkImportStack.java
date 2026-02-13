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

import com.google.common.io.CharStreams;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.cdk.lambdalayer.kubectl.v32.KubectlV32Layer;
import software.amazon.awscdk.services.ec2.SubnetSelection;
import software.amazon.awscdk.services.eks.AwsAuthMapping;
import software.amazon.awscdk.services.eks.Cluster;
import software.amazon.awscdk.services.eks.FargateCluster;
import software.amazon.awscdk.services.eks.FargateProfile;
import software.amazon.awscdk.services.eks.FargateProfileOptions;
import software.amazon.awscdk.services.eks.KubernetesManifest;
import software.amazon.awscdk.services.eks.KubernetesVersion;
import software.amazon.awscdk.services.eks.Selector;
import software.amazon.awscdk.services.eks.ServiceAccount;
import software.amazon.awscdk.services.eks.ServiceAccountOptions;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.iam.Role;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.logs.ILogGroup;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.amazon.awscdk.services.stepfunctions.Choice;
import software.amazon.awscdk.services.stepfunctions.Condition;
import software.amazon.awscdk.services.stepfunctions.CustomState;
import software.amazon.awscdk.services.stepfunctions.DefinitionBody;
import software.amazon.awscdk.services.stepfunctions.Fail;
import software.amazon.awscdk.services.stepfunctions.Pass;
import software.amazon.awscdk.services.stepfunctions.StateMachine;
import software.amazon.awscdk.services.stepfunctions.TaskInput;
import software.amazon.awscdk.services.stepfunctions.tasks.SnsPublish;
import software.constructs.Construct;

import sleeper.cdk.artefacts.SleeperArtefacts;
import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.cdk.stack.SleeperCoreStacks;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.DockerDeployment;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.EnvironmentUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static sleeper.cdk.util.Utils.createStateMachineLogOptions;
import static sleeper.core.properties.instance.BulkImportProperty.BULK_IMPORT_STARTER_LAMBDA_MEMORY;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_JOB_QUEUE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.EKSProperty.EKS_CLUSTER_ADMIN_ROLES;

/**
 * Deploys an EKS cluster and associated Kubernetes resources needed to run Spark on Kubernetes. In addition to this,
 * it creates a state machine which can run bulk import jobs on the cluster.
 */
public final class EksBulkImportStack extends NestedStack {
    private final Queue bulkImportJobQueue;

    public EksBulkImportStack(
            Construct scope, String id, InstanceProperties instanceProperties, SleeperArtefacts artefacts,
            BulkImportBucketStack importBucketStack, SleeperCoreStacks coreStacks) {
        super(scope, id);

        String instanceId = Utils.cleanInstanceId(instanceProperties);

        Queue queueForDLs = Queue.Builder
                .create(this, "BulkImportEKSJobDeadLetterQueue")
                .queueName(String.join("sleeper", instanceId, "BulkImportEKSDLQ"))
                .build();

        DeadLetterQueue deadLetterQueue = DeadLetterQueue.builder()
                .maxReceiveCount(1)
                .queue(queueForDLs)
                .build();

        coreStacks.alarmOnDeadLetters(this, "BulkImportEKSUndeliveredJobsAlarm", "passing bulk import jobs to the state machine for EKS", queueForDLs);

        bulkImportJobQueue = Queue.Builder
                .create(this, "BulkImportEKSJobQueue")
                .deadLetterQueue(deadLetterQueue)
                .visibilityTimeout(Duration.minutes(3))
                .queueName(String.join("sleeper", instanceId, "BulkImportEKSQ"))
                .build();

        instanceProperties.set(BULK_IMPORT_EKS_JOB_QUEUE_URL, bulkImportJobQueue.getQueueUrl());
        instanceProperties.set(BULK_IMPORT_EKS_JOB_QUEUE_ARN, bulkImportJobQueue.getQueueArn());
        bulkImportJobQueue.grantSendMessages(coreStacks.getIngestByQueuePolicyForGrants());
        bulkImportJobQueue.grantPurge(coreStacks.getPurgeQueuesPolicyForGrants());

        Map<String, String> env = EnvironmentUtils.createDefaultEnvironment(instanceProperties);
        env.put("BULK_IMPORT_PLATFORM", "EKS");
        SleeperLambdaCode lambdaCode = artefacts.lambdaCode(this);

        String functionName = String.join("-", "sleeper", instanceId, "bulk-import-eks-starter");

        IFunction bulkImportJobStarter = lambdaCode.buildFunction(this, LambdaHandler.BULK_IMPORT_STARTER, "BulkImportEKSJobStarter", builder -> builder
                .functionName(functionName)
                .description("Function to start EKS bulk import jobs")
                .memorySize(instanceProperties.getInt(BULK_IMPORT_STARTER_LAMBDA_MEMORY))
                .timeout(Duration.minutes(2))
                .environment(env)
                .logGroup(coreStacks.getLogGroup(LogGroupRef.BULK_IMPORT_EKS_STARTER))
                .events(List.of(SqsEventSource.Builder.create(bulkImportJobQueue).batchSize(1).build())));
        configureJobStarterFunction(bulkImportJobStarter);

        importBucketStack.getImportBucket().grantReadWrite(bulkImportJobStarter);
        coreStacks.grantValidateBulkImport(bulkImportJobStarter.getRole());

        String uniqueBulkImportId = String.join("-", "sleeper", instanceId, "bulk-import-eks");
        Cluster bulkImportCluster = FargateCluster.Builder.create(this, "EksBulkImportCluster")
                .clusterName(uniqueBulkImportId)
                .version(KubernetesVersion.V1_32)
                .kubectlLayer(new KubectlV32Layer(this, "KubectlLayer"))
                .vpc(coreStacks.getVpc())
                .vpcSubnets(List.of(SubnetSelection.builder().subnets(coreStacks.getSubnets()).build()))
                .build();

        instanceProperties.set(CdkDefinedInstanceProperty.BULK_IMPORT_EKS_CLUSTER_ENDPOINT, bulkImportCluster.getClusterEndpoint());

        KubernetesManifest namespace = createNamespace(bulkImportCluster, uniqueBulkImportId);
        instanceProperties.set(CdkDefinedInstanceProperty.BULK_IMPORT_EKS_NAMESPACE, uniqueBulkImportId);

        FargateProfile fargateProfile = bulkImportCluster.addFargateProfile("EksBulkImportFargateProfile", FargateProfileOptions.builder()
                .fargateProfileName(uniqueBulkImportId)
                .vpc(coreStacks.getVpc())
                .subnetSelection(SubnetSelection.builder()
                        .subnets(List.of(coreStacks.getSubnets().get(0)))
                        .build())
                .selectors(List.of(Selector.builder()
                        .namespace(uniqueBulkImportId)
                        .build()))
                .build());
        addFluentBitLogging(bulkImportCluster, fargateProfile, coreStacks.getLogGroup(LogGroupRef.BULK_IMPORT_EKS));

        ServiceAccount sparkSubmitServiceAccount = bulkImportCluster.addServiceAccount("SparkSubmitServiceAccount", ServiceAccountOptions.builder()
                .namespace(uniqueBulkImportId)
                .name("spark-submit")
                .build());

        ServiceAccount sparkServiceAccount = bulkImportCluster.addServiceAccount("SparkServiceAccount", ServiceAccountOptions.builder()
                .namespace(uniqueBulkImportId)
                .name("spark")
                .build());

        List.of(sparkServiceAccount, sparkSubmitServiceAccount)
                .forEach(sa -> sa.getNode().addDependency(namespace));
        coreStacks.grantIngest(sparkServiceAccount.getRole());
        coreStacks.grantReadWritePartitions(sparkServiceAccount.getRole());

        StateMachine stateMachine = createStateMachine(bulkImportCluster, instanceProperties, coreStacks);
        instanceProperties.set(CdkDefinedInstanceProperty.BULK_IMPORT_EKS_STATE_MACHINE_ARN, stateMachine.getStateMachineArn());

        bulkImportCluster.getAwsAuth().addRoleMapping(stateMachine.getRole(), AwsAuthMapping.builder()
                .groups(List.of())
                .build());
        addClusterAdminRoles(bulkImportCluster, instanceProperties);

        addRoleManifests(bulkImportCluster, namespace, uniqueBulkImportId, stateMachine.getRole());

        importBucketStack.getImportBucket().grantReadWrite(sparkServiceAccount);
        stateMachine.grantStartExecution(bulkImportJobStarter);

        Utils.addTags(this, instanceProperties);
    }

    private static void configureJobStarterFunction(IFunction bulkImportJobStarter) {

        bulkImportJobStarter.addToRolePolicy(PolicyStatement.Builder.create()
                .actions(List.of("eks:*", "states:*"))
                .effect(Effect.ALLOW)
                .resources(List.of("*"))
                .build());
    }

    private StateMachine createStateMachine(Cluster cluster, InstanceProperties instanceProperties, SleeperCoreStacks coreStacks) {
        String imageName = DockerDeployment.EKS_BULK_IMPORT.getDockerImageName(instanceProperties);

        Map<String, Object> runJobState = parseEksStepDefinition(
                "/step-functions/run-job.json", instanceProperties, cluster,
                replacements(Map.of("image-placeholder", imageName)));

        // Deleting the driver pod is necessary as a Spark job does not delete the pod afterwards:
        // https://spark.apache.org/docs/3.3.1/running-on-kubernetes.html#how-it-works
        // Although the Spark documentation says it doesn't use up resources in the completed state, it does when it's
        // scheduled into AWS Fargate.
        Map<String, Object> deleteDriverPodState = parseEksStepDefinition(
                "/step-functions/delete-driver-pod.json", instanceProperties, cluster);

        Map<String, Object> deleteJobState = parseEksStepDefinition(
                "/step-functions/delete-job.json", instanceProperties, cluster);

        SnsPublish publishError = SnsPublish.Builder
                .create(this, "AlertUserFailedSparkSubmit")
                .message(TaskInput.fromJsonPathAt("$.errorMessage"))
                .topic(coreStacks.getAlertsTopic())
                .build();

        Pass createErrorMessage = Pass.Builder.create(this, "CreateErrorMessage")
                .parameters(Map.of("errorMessage.$",
                        "States.Format('Bulk import job {} failed. Check the pod logs for details.', $.job.id)"))
                .build();

        return StateMachine.Builder.create(this, "EksBulkImportStateMachine")
                .definitionBody(DefinitionBody.fromChainable(
                        CustomState.Builder.create(this, "RunSparkJob").stateJson(runJobState).build()
                                .next(Choice.Builder.create(this, "SuccessDecision").build()
                                        .when(Condition.stringMatches("$.output.logs[0]", "*exit code: 0*"),
                                                CustomState.Builder.create(this, "DeleteDriverPod")
                                                        .stateJson(deleteDriverPodState).build()
                                                        .next(CustomState.Builder.create(this, "DeleteJob")
                                                                .stateJson(deleteJobState).build()))
                                        .otherwise(createErrorMessage.next(publishError).next(Fail.Builder
                                                .create(this, "FailedJobState").cause("Spark job failed").build())))))
                .logs(createStateMachineLogOptions(coreStacks.getLogGroup(LogGroupRef.BULK_IMPORT_EKS_STATE_MACHINE)))
                .build();
    }

    @SuppressWarnings("unchecked")
    private void addFluentBitLogging(Cluster cluster, FargateProfile fargateProfile, ILogGroup logGroup) {
        // Based on guide at https://docs.aws.amazon.com/eks/latest/userguide/fargate-logging.html

        KubernetesManifest namespace = cluster.addManifest("LoggingNamespace", Map.of(
                "apiVersion", "v1",
                "kind", "Namespace",
                "metadata", Map.of(
                        "name", "aws-observability",
                        "labels", Map.of("aws-observability", "enabled"))));

        // Fluent Bit configuration
        // See https://docs.fluentbit.io/manual/pipeline/outputs/cloudwatch
        Function<String, String> outputReplacements = replacements(Map.of(
                "region-placeholder", cluster.getStack().getRegion(),
                "log-group-placeholder", logGroup.getLogGroupName()));
        withDependencyOn(namespace, cluster.addManifest("LoggingConfig", Map.of(
                "apiVersion", "v1",
                "kind", "ConfigMap",
                "metadata", Map.of("name", "aws-logging", "namespace", "aws-observability"),
                "data", Map.of(
                        "flb_log_cw", "false",
                        "filters.conf", loadResource("/fluentbit/filters.conf"),
                        "output.conf", outputReplacements.apply(loadResource("/fluentbit/output.conf")),
                        "parsers.conf", loadResource("/fluentbit/parsers.conf")))));

        fargateProfile.getPodExecutionRole().addToPrincipalPolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of(
                        "logs:CreateLogStream",
                        "logs:CreateLogGroup",
                        "logs:DescribeLogStreams",
                        "logs:PutLogEvents",
                        "logs:PutRetentionPolicy"))
                .resources(List.of("*"))
                .build());
    }

    @SuppressWarnings("unchecked")
    private KubernetesManifest createNamespace(Cluster cluster, String namespaceName) {
        return cluster.addManifest("EksBulkImportNamespace", parseJson("/k8s/namespace.json", namespaceReplacement(namespaceName)));
    }

    private void addClusterAdminRoles(Cluster cluster, InstanceProperties properties) {
        List<String> roles = properties.getList(EKS_CLUSTER_ADMIN_ROLES);
        if (roles == null) {
            return;
        }
        for (String role : roles) {
            cluster.getAwsAuth().addMastersRole(Role.fromRoleName(this, "ClusterAccessFor" + role, role));
        }
    }

    @SuppressWarnings("unchecked")
    private void addRoleManifests(Cluster cluster, KubernetesManifest namespace, String namespaceName,
            IRole stateMachineRole) {
        withDependencyOn(namespace,
                cluster.addManifest("SparkSubmitRole", parseJson("/k8s/spark-submit-role.json", namespaceReplacement(namespaceName))),
                cluster.addManifest("SparkSubmitRoleBinding", parseJson("/k8s/spark-submit-role-binding.json", namespaceReplacement(namespaceName))),
                cluster.addManifest("SparkRole", parseJson("/k8s/spark-role.json", namespaceReplacement(namespaceName))),
                cluster.addManifest("SparkRoleBinding", parseJson("/k8s/spark-role-binding.json", namespaceReplacement(namespaceName))),
                cluster.addManifest("StepFunctionRole", parseJson("/k8s/step-function-role.json", namespaceReplacement(namespaceName))),
                cluster.addManifest("StepFunctionRoleBinding", parseJson("/k8s/step-function-role-binding.json",
                        namespaceReplacement(namespaceName).andThen(replacement("user-placeholder", stateMachineRole.getRoleArn())))));
    }

    private void withDependencyOn(KubernetesManifest namespace, KubernetesManifest... manifests) {
        for (KubernetesManifest manifest : manifests) {
            manifest.getNode().addDependency(namespace);
        }
    }

    private static Map<String, Object> parseEksStepDefinition(String resource, InstanceProperties instanceProperties, Cluster cluster) {
        return parseEksStepDefinition(resource, instanceProperties, cluster, json -> json);
    }

    private static Map<String, Object> parseEksStepDefinition(
            String resource, InstanceProperties instanceProperties, Cluster cluster, Function<String, String> replacements) {
        return parseJson(resource,
                namespaceReplacement(instanceProperties.get(CdkDefinedInstanceProperty.BULK_IMPORT_EKS_NAMESPACE))
                        .andThen(replacements(Map.of(
                                "endpoint-placeholder", instanceProperties.get(CdkDefinedInstanceProperty.BULK_IMPORT_EKS_CLUSTER_ENDPOINT),
                                "cluster-placeholder", cluster.getClusterName(),
                                "ca-placeholder", cluster.getClusterCertificateAuthorityData())))
                        .andThen(replacements));
    }

    private static Map<String, Object> parseJson(
            String resource, Function<String, String> replacements) {
        String json = loadResource(resource);
        String jsonWithReplacements = replacements.apply(json);
        return new Gson().fromJson(jsonWithReplacements, new JsonTypeToken());
    }

    private static String loadResource(String resource) {
        try (InputStream is = EksBulkImportStack.class.getResourceAsStream(resource)) {
            return CharStreams.toString(new InputStreamReader(is, StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Function<String, String> namespaceReplacement(String namespace) {
        return replacement("namespace-placeholder", namespace);
    }

    private static Function<String, String> replacement(String key, String value) {
        return str -> str.replace(key, value);
    }

    private static Function<String, String> replacements(Map<String, String> replacements) {
        return str -> {
            for (Map.Entry<String, String> replacement : replacements.entrySet()) {
                str = str.replace(replacement.getKey(), replacement.getValue());
            }
            return str;
        };
    }

    public Queue getBulkImportJobQueue() {
        return bulkImportJobQueue;
    }

    public static class JsonTypeToken extends TypeToken<Map<String, Object>> {
    }
}
