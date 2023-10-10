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
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.io.IOUtils;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.cdk.lambdalayer.kubectl.v24.KubectlV24Layer;
import software.amazon.awscdk.services.cloudwatch.ComparisonOperator;
import software.amazon.awscdk.services.cloudwatch.CreateAlarmOptions;
import software.amazon.awscdk.services.cloudwatch.MetricOptions;
import software.amazon.awscdk.services.cloudwatch.TreatMissingData;
import software.amazon.awscdk.services.cloudwatch.actions.SnsAction;
import software.amazon.awscdk.services.ec2.ISubnet;
import software.amazon.awscdk.services.ec2.IVpc;
import software.amazon.awscdk.services.ec2.Subnet;
import software.amazon.awscdk.services.ec2.SubnetSelection;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ec2.VpcLookupOptions;
import software.amazon.awscdk.services.eks.AwsAuthMapping;
import software.amazon.awscdk.services.eks.Cluster;
import software.amazon.awscdk.services.eks.FargateCluster;
import software.amazon.awscdk.services.eks.FargateClusterProps;
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
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sns.ITopic;
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

import sleeper.cdk.Utils;
import sleeper.cdk.jars.BuiltJar;
import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.jars.LambdaCode;
import sleeper.cdk.stack.IngestStatusStoreStack;
import sleeper.cdk.stack.StateStoreStacks;
import sleeper.cdk.stack.TableDataStack;
import sleeper.cdk.stack.TopicStack;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.CdkDefinedInstanceProperty;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static sleeper.cdk.stack.IngestStack.addIngestSourceBucketReferences;
import static sleeper.configuration.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.LOG_RETENTION_IN_DAYS;
import static sleeper.configuration.properties.instance.CommonProperty.REGION;
import static sleeper.configuration.properties.instance.CommonProperty.SUBNETS;
import static sleeper.configuration.properties.instance.CommonProperty.VPC_ID;
import static sleeper.configuration.properties.instance.EKSProperty.BULK_IMPORT_REPO;
import static sleeper.configuration.properties.instance.EKSProperty.EKS_CLUSTER_ADMIN_ROLES;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_JOB_QUEUE_ARN;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_JOB_QUEUE_URL;

/**
 * An {@link EksBulkImportStack} creates an EKS cluster and associated Kubernetes
 * resources needed to run Spark on Kubernetes. In addition to this, it creates
 * a statemachine which can run jobs on the cluster.
 */
public final class EksBulkImportStack extends NestedStack {
    private final StateMachine stateMachine;
    private final ServiceAccount sparkServiceAccount;
    private final Queue bulkImportJobQueue;

    public EksBulkImportStack(
            Construct scope,
            String id,
            InstanceProperties instanceProperties,
            BuiltJars jars,
            BulkImportBucketStack importBucketStack,
            StateStoreStacks stateStoreStacks, TableDataStack dataStack,
            TopicStack errorsTopicStack,
            IngestStatusStoreStack statusStoreStack) {
        super(scope, id);

        List<IBucket> ingestSourceBuckets = addIngestSourceBucketReferences(this, "IngestBucket", instanceProperties);

        String instanceId = instanceProperties.get(ID);

        Queue queueForDLs = Queue.Builder
                .create(this, "BulkImportEKSJobDeadLetterQueue")
                .queueName(instanceId + "-BulkImportEKSDLQ")
                .build();

        DeadLetterQueue deadLetterQueue = DeadLetterQueue.builder()
                .maxReceiveCount(1)
                .queue(queueForDLs)
                .build();

        queueForDLs.metricApproximateNumberOfMessagesVisible().with(MetricOptions.builder()
                        .period(Duration.seconds(60))
                        .statistic("Sum")
                        .build())
                .createAlarm(this, "BulkImportEKSUndeliveredJobsAlarm", CreateAlarmOptions.builder()
                        .alarmDescription("Alarms if there are any messages that have failed validation or failed to be passed to the statemachine")
                        .evaluationPeriods(1)
                        .comparisonOperator(ComparisonOperator.GREATER_THAN_THRESHOLD)
                        .threshold(0)
                        .datapointsToAlarm(1)
                        .treatMissingData(TreatMissingData.IGNORE)
                        .build())
                .addAlarmAction(new SnsAction(errorsTopicStack.getTopic()));

        bulkImportJobQueue = Queue.Builder
                .create(this, "BulkImportEKSJobQueue")
                .deadLetterQueue(deadLetterQueue)
                .visibilityTimeout(Duration.minutes(3))
                .queueName(instanceId + "-BulkImportEKSQ")
                .build();

        instanceProperties.set(BULK_IMPORT_EKS_JOB_QUEUE_URL, bulkImportJobQueue.getQueueUrl());
        instanceProperties.set(BULK_IMPORT_EKS_JOB_QUEUE_ARN, bulkImportJobQueue.getQueueArn());

        Map<String, String> env = Utils.createDefaultEnvironment(instanceProperties);
        env.put("BULK_IMPORT_PLATFORM", "EKS");
        IBucket jarsBucket = Bucket.fromBucketName(this, "CodeBucketEKS", instanceProperties.get(JARS_BUCKET));
        LambdaCode bulkImportStarterJar = jars.lambdaCode(BuiltJar.BULK_IMPORT_STARTER, jarsBucket);

        IBucket configBucket = Bucket.fromBucketName(this, "ConfigBucket", instanceProperties.get(CdkDefinedInstanceProperty.CONFIG_BUCKET));

        String functionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceId.toLowerCase(Locale.ROOT), "eks-bulk-import-job-starter"));

        IFunction bulkImportJobStarter = bulkImportStarterJar.buildFunction(this, "BulkImportEKSJobStarter", builder -> builder
                .functionName(functionName)
                .description("Function to start EKS bulk import jobs")
                .memorySize(1024)
                .timeout(Duration.minutes(2))
                .environment(env)
                .runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_11)
                .handler("sleeper.bulkimport.starter.BulkImportStarterLambda")
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS)))
                .events(Lists.newArrayList(SqsEventSource.Builder.create(bulkImportJobQueue).batchSize(1).build())));
        configureJobStarterFunction(bulkImportJobStarter);

        configBucket.grantRead(bulkImportJobStarter);
        importBucketStack.getImportBucket().grantReadWrite(bulkImportJobStarter);
        ingestSourceBuckets.forEach(bucket -> bucket.grantRead(bulkImportJobStarter));
        statusStoreStack.getResources().grantWriteJobEvent(bulkImportJobStarter.getRole());
        stateStoreStacks.grantReadPartitions(bulkImportJobStarter);

        VpcLookupOptions vpcLookupOptions = VpcLookupOptions.builder()
                .vpcId(instanceProperties.get(VPC_ID))
                .build();
        IVpc vpc = Vpc.fromLookup(this, "VPC", vpcLookupOptions);

        Cluster bulkImportCluster = new FargateCluster(this, "EksBulkImportCluster", FargateClusterProps.builder()
                .clusterName(String.join("-", "sleeper", instanceId.toLowerCase(Locale.ROOT), "eksBulkImportCluster"))
                .version(KubernetesVersion.of("1.24"))
                .kubectlLayer(new KubectlV24Layer(this, "KubectlLayer"))
                .vpc(vpc)
                .vpcSubnets(Lists.newArrayList(SubnetSelection.builder().subnets(vpc.getPrivateSubnets()).build()))
                .build());

        instanceProperties.set(CdkDefinedInstanceProperty.BULK_IMPORT_EKS_CLUSTER_ENDPOINT, bulkImportCluster.getClusterEndpoint());

        String uniqueBulkImportId = Utils.truncateToMaxSize("sleeper-" + instanceProperties.get(ID)
                .replace(".", "-") + "-eks-bulk-import", 63);

        KubernetesManifest namespace = createNamespace(bulkImportCluster, uniqueBulkImportId);
        instanceProperties.set(CdkDefinedInstanceProperty.BULK_IMPORT_EKS_NAMESPACE, uniqueBulkImportId);

        ISubnet subnet = Subnet.fromSubnetId(this, "EksBulkImportSubnet", instanceProperties.getList(SUBNETS).get(0));
        bulkImportCluster.addFargateProfile("EksBulkImportFargateProfile", FargateProfileOptions.builder()
                .fargateProfileName(uniqueBulkImportId)
                .vpc(vpc)
                .subnetSelection(SubnetSelection.builder()
                        .subnets(List.of(subnet))
                        .build())
                .selectors(Lists.newArrayList(Selector.builder()
                        .namespace(uniqueBulkImportId)
                        .build()))
                .build());

        ServiceAccount sparkSubmitServiceAccount = bulkImportCluster.addServiceAccount("SparkSubmitServiceAccount", ServiceAccountOptions.builder()
                .namespace(uniqueBulkImportId)
                .name("spark-submit")
                .build());

        this.sparkServiceAccount = bulkImportCluster.addServiceAccount("SparkServiceAccount", ServiceAccountOptions.builder()
                .namespace(uniqueBulkImportId)
                .name("spark")
                .build());

        Lists.newArrayList(sparkServiceAccount, sparkSubmitServiceAccount)
                .forEach(sa -> sa.getNode().addDependency(namespace));
        grantAccesses(stateStoreStacks, dataStack, configBucket);

        this.stateMachine = createStateMachine(bulkImportCluster, instanceProperties, errorsTopicStack.getTopic());
        instanceProperties.set(CdkDefinedInstanceProperty.BULK_IMPORT_EKS_STATE_MACHINE_ARN, stateMachine.getStateMachineArn());

        bulkImportCluster.getAwsAuth().addRoleMapping(stateMachine.getRole(), AwsAuthMapping.builder()
                .groups(Lists.newArrayList())
                .build());
        addClusterAdminRoles(bulkImportCluster, instanceProperties);

        createManifests(bulkImportCluster, namespace, uniqueBulkImportId, stateMachine.getRole());

        importBucketStack.getImportBucket().grantReadWrite(sparkServiceAccount);
        ingestSourceBuckets.forEach(bucket -> grantAccessToResources(bulkImportJobStarter, bucket));
        statusStoreStack.getResources().grantWriteJobEvent(sparkServiceAccount);

        Utils.addStackTagIfSet(this, instanceProperties);
    }

    private static void configureJobStarterFunction(IFunction bulkImportJobStarter) {

        bulkImportJobStarter.addToRolePolicy(PolicyStatement.Builder.create()
                .actions(Lists.newArrayList("eks:*", "states:*"))
                .effect(Effect.ALLOW)
                .resources(Lists.newArrayList("*"))
                .build());
    }

    private StateMachine createStateMachine(Cluster cluster, InstanceProperties instanceProperties,
                                            ITopic errorsTopic) {
        String imageName = instanceProperties.get(ACCOUNT) +
                ".dkr.ecr." +
                instanceProperties.get(REGION) +
                ".amazonaws.com/" +
                instanceProperties.get(BULK_IMPORT_REPO) +
                ":" +
                instanceProperties.get(CdkDefinedInstanceProperty.VERSION);

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
                .topic(errorsTopic)
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
                                                .create(this, "FailedJobState").cause("Spark job failed").build()))))
                ).build();
    }

    private void grantAccesses(StateStoreStacks stateStoreStacks, TableDataStack dataStack, IBucket configBucket) {
        dataStack.getDataBucket().grantReadWrite(sparkServiceAccount);
        stateStoreStacks.grantReadPartitionsReadWriteActiveFiles(sparkServiceAccount);
        configBucket.grantRead(sparkServiceAccount);
    }

    private KubernetesManifest createNamespace(Cluster bulkImportCluster, String bulkImportNamespace) {
        return createManifestFromResource(bulkImportCluster, "EksBulkImportNamespace", bulkImportNamespace,
                "/k8s/namespace.json");
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

    private void createManifests(Cluster cluster, KubernetesManifest namespace, String namespaceName,
                                 IRole stateMachineRole) {
        Lists.newArrayList(
                        createManifestFromResource(cluster, "SparkSubmitRole", namespaceName, "/k8s/spark-submit-role.json"),
                        createManifestFromResource(cluster, "SparkSubmitRoleBinding", namespaceName,
                                "/k8s/spark-submit-role-binding.json"),
                        createManifestFromResource(cluster, "SparkRole", namespaceName, "/k8s/spark-role.json"),
                        createManifestFromResource(cluster, "SparkRoleBinding", namespaceName, "/k8s/spark-role-binding.json"),
                        createManifestFromResource(cluster, "StepFunctionRole", namespaceName, "/k8s/step-function-role.json"),
                        createManifestFromResource(cluster, "StepFunctionRoleBinding", namespaceName,
                                "/k8s/step-function-role-binding.json",
                                replacements(Map.of("user-placeholder", stateMachineRole.getRoleArn()))))
                .forEach(manifest -> manifest.getNode().addDependency(namespace));
    }

    private static KubernetesManifest createManifestFromResource(Cluster cluster, String id, String namespace, String resource) {
        return createManifestFromResource(cluster, id, namespace, resource, json -> json);
    }

    private static KubernetesManifest createManifestFromResource(Cluster cluster, String id, String namespace, String resource,
                                                                 Function<String, String> replacements) {
        return cluster.addManifest(id, parseJsonWithNamespace(resource, namespace, replacements));
    }

    private static Map<String, Object> parseEksStepDefinition(String resource, InstanceProperties instanceProperties, Cluster cluster) {
        return parseEksStepDefinition(resource, instanceProperties, cluster, json -> json);
    }

    private static Map<String, Object> parseEksStepDefinition(
            String resource, InstanceProperties instanceProperties, Cluster cluster, Function<String, String> replacements) {
        return parseJsonWithNamespace(resource,
                instanceProperties.get(CdkDefinedInstanceProperty.BULK_IMPORT_EKS_NAMESPACE),
                replacements(Map.of(
                        "endpoint-placeholder", instanceProperties.get(CdkDefinedInstanceProperty.BULK_IMPORT_EKS_CLUSTER_ENDPOINT),
                        "cluster-placeholder", cluster.getClusterName(),
                        "ca-placeholder", cluster.getClusterCertificateAuthorityData()))
                        .andThen(replacements));
    }

    private static Map<String, Object> parseJsonWithNamespace(
            String resource, String namespace, Function<String, String> replacements) {
        return parseJson(resource, replacement("namespace-placeholder", namespace).andThen(replacements));
    }

    private static Map<String, Object> parseJson(
            String resource, Function<String, String> replacements) {
        String json;
        try {
            json = IOUtils.toString(Objects.requireNonNull(EksBulkImportStack.class.getResourceAsStream(resource)), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        String jsonWithReplacements = replacements.apply(json);
        return new Gson().fromJson(jsonWithReplacements, new JsonTypeToken());
    }

    private static Function<String, String> replacement(String key, String value) {
        return json -> json.replace(key, value);
    }

    private static Function<String, String> replacements(Map<String, String> replacements) {
        return json -> {
            for (Map.Entry<String, String> replacement : replacements.entrySet()) {
                json = json.replace(replacement.getKey(), replacement.getValue());
            }
            return json;
        };
    }

    public void grantAccessToResources(IFunction starterFunction, IBucket ingestBucket) {
        stateMachine.grantStartExecution(starterFunction);
        if (ingestBucket != null) {
            ingestBucket.grantRead(sparkServiceAccount);
        }
    }

    public Queue getBulkImportJobQueue() {
        return bulkImportJobQueue;
    }

    public static class JsonTypeToken extends TypeToken<Map<String, Object>> {
    }
}
