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

import com.facebook.collections.Pair;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.commons.io.IOUtils;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.cdk.lambdalayer.kubectl.v24.KubectlV24Layer;
import software.amazon.awscdk.services.cloudwatch.ComparisonOperator;
import software.amazon.awscdk.services.cloudwatch.CreateAlarmOptions;
import software.amazon.awscdk.services.cloudwatch.MetricOptions;
import software.amazon.awscdk.services.cloudwatch.TreatMissingData;
import software.amazon.awscdk.services.cloudwatch.actions.SnsAction;
import software.amazon.awscdk.services.ec2.IVpc;
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
import software.amazon.awscdk.services.stepfunctions.CustomStateProps;
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
import sleeper.cdk.stack.StateStoreStack;
import sleeper.cdk.stack.TableStack;
import sleeper.cdk.stack.TopicStack;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.SystemDefinedInstanceProperty;
import sleeper.configuration.properties.UserDefinedInstanceProperty;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static sleeper.cdk.stack.IngestStack.addIngestSourceBucketReferences;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_EKS_JOB_QUEUE_URL;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;

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
            TableStack tableStack,
            TopicStack errorsTopicStack,
            IngestStatusStoreStack statusStoreStack,
            List<StateStoreStack> stateStoreStacks) {
        super(scope, id);

        List<IBucket> ingestSourceBuckets = addIngestSourceBucketReferences(this, "IngestBucket", instanceProperties);

        String instanceId = instanceProperties.get(UserDefinedInstanceProperty.ID);

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
                .queueName(instanceId + "-BulkImportEKSQ")
                .build();

        instanceProperties.set(BULK_IMPORT_EKS_JOB_QUEUE_URL, bulkImportJobQueue.getQueueUrl());

        Map<String, String> env = Utils.createDefaultEnvironment(instanceProperties);
        env.put("BULK_IMPORT_PLATFORM", "EKS");
        IBucket jarsBucket = Bucket.fromBucketName(this, "CodeBucketEKS", instanceProperties.get(JARS_BUCKET));
        LambdaCode bulkImportStarterJar = jars.lambdaCode(BuiltJar.BULK_IMPORT_STARTER, jarsBucket);

        IBucket configBucket = Bucket.fromBucketName(this, "ConfigBucket", instanceProperties.get(SystemDefinedInstanceProperty.CONFIG_BUCKET));

        String functionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceId.toLowerCase(Locale.ROOT), "eks-bulk-import-job-starter"));

        IFunction bulkImportJobStarter = bulkImportStarterJar.buildFunction(this, "BulkImportEKSJobStarter", builder -> builder
                .functionName(functionName)
                .description("Function to start EKS bulk import jobs")
                .memorySize(1024)
                .timeout(Duration.seconds(10))
                .environment(env)
                .runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_11)
                .handler("sleeper.bulkimport.starter.BulkImportStarter")
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(UserDefinedInstanceProperty.LOG_RETENTION_IN_DAYS)))
                .events(Lists.newArrayList(new SqsEventSource(bulkImportJobQueue))));
        configureJobStarterFunction(bulkImportJobStarter);

        configBucket.grantRead(bulkImportJobStarter);
        importBucketStack.getImportBucket().grantReadWrite(bulkImportJobStarter);
        ingestSourceBuckets.forEach(bucket -> bucket.grantRead(bulkImportJobStarter));
        statusStoreStack.getResources().grantWriteJobEvent(bulkImportJobStarter.getRole());
        stateStoreStacks.forEach(sss -> sss.grantReadPartitionMetadata(bulkImportJobStarter));

        VpcLookupOptions vpcLookupOptions = VpcLookupOptions.builder()
                .vpcId(instanceProperties.get(UserDefinedInstanceProperty.VPC_ID))
                .build();
        IVpc vpc = Vpc.fromLookup(this, "VPC", vpcLookupOptions);

        Cluster bulkImportCluster = new FargateCluster(this, "EksBulkImportCluster", FargateClusterProps.builder()
                .clusterName(String.join("-", "sleeper", instanceId.toLowerCase(Locale.ROOT), "eksBulkImportCluster"))
                .version(KubernetesVersion.of("1.24"))
                .kubectlLayer(new KubectlV24Layer(this, "KubectlLayer"))
                .vpc(vpc)
                .vpcSubnets(Lists.newArrayList(SubnetSelection.builder().subnets(vpc.getPrivateSubnets()).build()))
                .build());

        instanceProperties.set(SystemDefinedInstanceProperty.BULK_IMPORT_EKS_CLUSTER_ENDPOINT, bulkImportCluster.getClusterEndpoint());

        String uniqueBulkImportId = Utils.truncateToMaxSize("sleeper-" + instanceProperties.get(UserDefinedInstanceProperty.ID)
                .replace(".", "-") + "-eks-bulk-import", 63);

        KubernetesManifest namespace = createNamespace(bulkImportCluster, uniqueBulkImportId);
        instanceProperties.set(SystemDefinedInstanceProperty.BULK_IMPORT_EKS_NAMESPACE, uniqueBulkImportId);

        bulkImportCluster.addFargateProfile("EksBulkImportFargateProfile", FargateProfileOptions.builder()
                .fargateProfileName(uniqueBulkImportId)
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
        grantAccesses(tableStack.getDataBuckets(), tableStack.getStateStoreStacks(), configBucket);

        this.stateMachine = createStateMachine(bulkImportCluster, instanceProperties, errorsTopicStack.getTopic());
        instanceProperties.set(SystemDefinedInstanceProperty.BULK_IMPORT_EKS_STATE_MACHINE_ARN, stateMachine.getStateMachineArn());

        bulkImportCluster.getAwsAuth().addRoleMapping(stateMachine.getRole(), AwsAuthMapping.builder()
                .groups(Lists.newArrayList())
                .build());

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
        String imageName = new StringBuilder()
                .append(instanceProperties.get(UserDefinedInstanceProperty.ACCOUNT))
                .append(".dkr.ecr.")
                .append(instanceProperties.get(UserDefinedInstanceProperty.REGION))
                .append(".amazonaws.com/")
                .append(instanceProperties.get(UserDefinedInstanceProperty.BULK_IMPORT_REPO))
                .append(":")
                .append(instanceProperties.get(SystemDefinedInstanceProperty.VERSION))
                .toString();

        String sparkJobJson = parseJsonFile("/step-functions/run-job.json",
                instanceProperties.get(SystemDefinedInstanceProperty.BULK_IMPORT_EKS_NAMESPACE));
        String parsedSparkJobStepFunction = sparkJobJson
                .replace("endpoint-placeholder", instanceProperties.get(SystemDefinedInstanceProperty.BULK_IMPORT_EKS_CLUSTER_ENDPOINT))
                .replace("image-placeholder", imageName)
                .replace("cluster-placeholder", cluster.getClusterName())
                .replace("ca-placeholder", cluster.getClusterCertificateAuthorityData());

        Map<String, Object> runJobState = new Gson().fromJson(parsedSparkJobStepFunction, Map.class);

        String deleteJobJson = parseJsonFile("/step-functions/delete-driver-pod.json",
                instanceProperties.get(SystemDefinedInstanceProperty.BULK_IMPORT_EKS_NAMESPACE));
        String parsedDeleteJob = deleteJobJson
                .replace("endpoint-placeholder", instanceProperties.get(SystemDefinedInstanceProperty.BULK_IMPORT_EKS_CLUSTER_ENDPOINT))
                .replace("image-placeholder", imageName)
                .replace("cluster-placeholder", cluster.getClusterName())
                .replace("ca-placeholder", cluster.getClusterCertificateAuthorityData());

        Map<String, Object> deleteJobState = new Gson().fromJson(parsedDeleteJob, Map.class);

        SnsPublish publishError = SnsPublish.Builder
                .create(this, "AlertUserFailedSparkSubmit")
                .message(TaskInput.fromJsonPathAt("$.errorMessage"))
                .topic(errorsTopic)
                .build();

        Map<String, String> createErrorMessageParams = new HashMap<>();
        createErrorMessageParams.put("errorMessage.$",
                "States.Format('Bulk import job {} failed. Check the pod logs for details.', $.job.jobId)");

        Pass createErrorMessage = Pass.Builder.create(this, "CreateErrorMessage").parameters(createErrorMessageParams)
                .build();

        return StateMachine.Builder.create(this, "EksBulkImportStateMachine")
                .definition(
                        new CustomState(this, "RunSparkJob", CustomStateProps.builder().stateJson(runJobState).build())
                                .next(Choice.Builder.create(this, "SuccessDecision").build()
                                        .when(Condition.stringMatches("$.output.logs[0]", "*exit code: 0*"),
                                                CustomState.Builder.create(this, "DeleteDriverPod")
                                                        .stateJson(deleteJobState).build())
                                        .otherwise(createErrorMessage.next(publishError).next(Fail.Builder
                                                .create(this, "FailedJobState").cause("Spark job failed").build()))))
                .build();
    }

    private void grantAccesses(List<IBucket> dataBuckets,
                               List<StateStoreStack> stateStoreStacks, IBucket configBucket) {
        dataBuckets.forEach(bucket -> bucket.grantReadWrite(sparkServiceAccount));
        stateStoreStacks.forEach(sss -> {
            sss.grantReadWriteFileInPartitionMetadata(sparkServiceAccount);
            sss.grantReadWriteFileLifecycleMetadata(sparkServiceAccount);
            sss.grantReadPartitionMetadata(sparkServiceAccount);
        });
        configBucket.grantRead(sparkServiceAccount);
    }

    private KubernetesManifest createNamespace(Cluster bulkImportCluster, String bulkImportNamespace) {
        return createManifestFromResource(bulkImportCluster, "EksBulkImportNamespace", bulkImportNamespace,
                "/k8s/namespace.json");
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
                                Pair.of("user-placeholder", stateMachineRole.getRoleArn())))
                .forEach(manifest -> manifest.getNode().addDependency(namespace));
    }

    private KubernetesManifest createManifestFromResource(Cluster cluster, String id, String namespace, String resource,
                                                          Pair<String, String>... replacements) {
        String json = parseJsonFile(resource, namespace);
        for (Pair<String, String> replacement : replacements) {
            json = json.replace(replacement.getFirst(), replacement.getSecond());
        }

        return cluster.addManifest(id, new Gson().fromJson(json, Map.class));
    }

    private String parseJsonFile(String resource, String namespace) {
        String json;
        try {
            json = IOUtils.toString(getClass().getResourceAsStream(resource), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return json.replace("namespace-placeholder", namespace);
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
}
