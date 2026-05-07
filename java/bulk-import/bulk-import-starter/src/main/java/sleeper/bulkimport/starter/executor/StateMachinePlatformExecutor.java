/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.bulkimport.starter.executor;

import com.google.gson.Gson;
import software.amazon.awssdk.services.sfn.SfnClient;

import sleeper.bulkimport.core.configuration.ConfigurationUtils;
import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.core.deploy.DockerDeployment;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.EmrInstanceArchitecture;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_CLUSTER_ENDPOINT;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_NAMESPACE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_STATE_MACHINE_ARN;
import static sleeper.core.properties.instance.EKSProperty.BULK_IMPORT_EKS_SPARK_DRIVER_MEMORY;
import static sleeper.core.properties.instance.EKSProperty.BULK_IMPORT_EKS_SPARK_DRIVER_MEMORY_OVERHEAD;
import static sleeper.core.properties.instance.EKSProperty.BULK_IMPORT_EKS_SPARK_DRIVER_SERVICE_ACCOUNT_NAME;
import static sleeper.core.properties.instance.EKSProperty.BULK_IMPORT_EKS_SPARK_EXECUTOR_INSTANCES;
import static sleeper.core.properties.instance.EKSProperty.BULK_IMPORT_EKS_SPARK_EXECUTOR_MEMORY;
import static sleeper.core.properties.instance.EKSProperty.BULK_IMPORT_EKS_SPARK_EXECUTOR_MEMORY_OVERHEAD;
import static sleeper.core.properties.instance.EKSProperty.BULK_IMPORT_EKS_SPARK_HADOOP_S3A_CREDENTIALS_PROVIDER;
import static sleeper.core.properties.instance.EKSProperty.BULK_IMPORT_EKS_SPARK_HADOOP_S3A_INPUT_FADVISE;
import static sleeper.core.properties.instance.EKSProperty.EKS_IS_NATIVE_LIBS_IMAGE;

/**
 * Starts a bulk import job on Spark in an EKS cluster via AWS Step Functions. It creates a list of arguments and
 * submits them to a pre-deployed state machine linked to EKS.
 */
public class StateMachinePlatformExecutor implements PlatformExecutor {
    private static final String SPARK_IMAGE_JAR_LOCATION = "local:///opt/spark/workdir/bulk-import-runner.jar";
    private static final String SPARK_IMAGE_JAVA_HOME = "/opt/java/openjdk";
    private static final String NATIVE_IMAGE_JAR_LOCATION = "local:///opt/spark/workdir/bulk-import-runner.jar";
    private static final String NATIVE_IMAGE_LOG4J_LOCATION = "file:///opt/spark/workdir/log4j.properties";
    private static final String NATIVE_IMAGE_JAVA_HOME = "/usr/lib/jvm/java-11-amazon-corretto";

    private final SfnClient stepFunctions;
    private final InstanceProperties instanceProperties;

    public StateMachinePlatformExecutor(SfnClient stepFunctions,
            InstanceProperties instanceProperties) {
        this.stepFunctions = stepFunctions;
        this.instanceProperties = instanceProperties;
    }

    @Override
    public void runJobOnPlatform(BulkImportArguments arguments) {
        String stateMachineArn = instanceProperties.get(BULK_IMPORT_EKS_STATE_MACHINE_ARN);
        BulkImportJob bulkImportJob = arguments.getBulkImportJob();
        Map<String, Object> input = new HashMap<>();
        List<String> args = constructArgs(arguments, stateMachineArn);
        input.put("job", bulkImportJob);
        input.put("jobPodPrefix", jobPodPrefix(bulkImportJob));
        input.put("args", args);
        String inputJson = new Gson().toJson(input);

        stepFunctions.startExecution(request -> request
                .stateMachineArn(stateMachineArn)
                .name(jobExecutionName(bulkImportJob))
                .input(inputJson));
    }

    private Map<String, String> getDefaultSparkConfig(BulkImportJob bulkImportJob) {
        Map<String, String> defaultConfig = new HashMap<>(ConfigurationUtils.getSparkConfigurationFromInstanceProperties(instanceProperties, EmrInstanceArchitecture.X86_64));
        String imageName = DockerDeployment.EKS_BULK_IMPORT.getDockerImageName(instanceProperties);
        defaultConfig.put("spark.master", "k8s://" + instanceProperties.get(BULK_IMPORT_EKS_CLUSTER_ENDPOINT));
        defaultConfig.put("spark.app.name", bulkImportJob.getId());
        defaultConfig.put("spark.kubernetes.container.image", imageName);
        defaultConfig.put("spark.kubernetes.namespace", instanceProperties.get(BULK_IMPORT_EKS_NAMESPACE));
        String jobPodPrefix = jobPodPrefix(bulkImportJob);
        defaultConfig.put("spark.kubernetes.driver.pod.name", jobPodPrefix);
        defaultConfig.put("spark.kubernetes.executor.podNamePrefix", jobPodPrefix);

        defaultConfig.put("spark.executor.instances", instanceProperties.get(BULK_IMPORT_EKS_SPARK_EXECUTOR_INSTANCES));
        defaultConfig.put("spark.driver.memory", instanceProperties.get(BULK_IMPORT_EKS_SPARK_DRIVER_MEMORY));
        defaultConfig.put("spark.executor.memory", instanceProperties.get(BULK_IMPORT_EKS_SPARK_EXECUTOR_MEMORY));
        defaultConfig.put("spark.driver.memoryOverhead", instanceProperties.get(BULK_IMPORT_EKS_SPARK_DRIVER_MEMORY_OVERHEAD));
        defaultConfig.put("spark.executor.memoryOverhead", instanceProperties.get(BULK_IMPORT_EKS_SPARK_EXECUTOR_MEMORY_OVERHEAD));
        defaultConfig.put("spark.kubernetes.authenticate.driver.serviceAccountName", instanceProperties.get(BULK_IMPORT_EKS_SPARK_DRIVER_SERVICE_ACCOUNT_NAME));
        defaultConfig.put("spark.hadoop.fs.s3a.aws.credentials.provider", instanceProperties.get(BULK_IMPORT_EKS_SPARK_HADOOP_S3A_CREDENTIALS_PROVIDER));
        defaultConfig.put("spark.hadoop.fs.s3a.experimental.input.fadvise", instanceProperties.get(BULK_IMPORT_EKS_SPARK_HADOOP_S3A_INPUT_FADVISE));

        return defaultConfig;
    }

    private List<String> constructArgs(BulkImportArguments arguments, String taskId) {
        BulkImportJob bulkImportJob = arguments.getBulkImportJob();
        Map<String, String> baseSparkConfig = getDefaultSparkConfig(bulkImportJob);

        // Point to locations in the Docker image
        String jarLocation;
        if (instanceProperties.getBoolean(EKS_IS_NATIVE_LIBS_IMAGE)) {
            baseSparkConfig.put("spark.executorEnv.JAVA_HOME", NATIVE_IMAGE_JAVA_HOME);
            baseSparkConfig.put("spark.driver.extraJavaOptions", "-Dlog4j.configuration=" + NATIVE_IMAGE_LOG4J_LOCATION);
            baseSparkConfig.put("spark.executor.extraJavaOptions", "-Dlog4j.configuration=" + NATIVE_IMAGE_LOG4J_LOCATION);
            jarLocation = NATIVE_IMAGE_JAR_LOCATION;
        } else {
            baseSparkConfig.put("spark.executorEnv.JAVA_HOME", SPARK_IMAGE_JAVA_HOME);
            jarLocation = SPARK_IMAGE_JAR_LOCATION;
        }

        return arguments.sparkSubmitCommandForEKSCluster(taskId, jarLocation, baseSparkConfig);
    }

    private static String jobPodPrefix(BulkImportJob job) {
        /*
         * Spark adds extra IDs to the end of this - up to 17 characters, and performs some extra validation:
         * - whether the pod name prefix is <= 47 characters
         * (https://spark.apache.org/docs/3.3.1/running-on-kubernetes.html)
         * - whether the pod name prefix starts with a letter
         * (https://kubernetes.io/docs/concepts/overview/working-with-objects/names/)
         * After adding a "job-" prefix, maximum id length = 47-(17+4) = 26 characters
         */
        if (job.getId().length() > 26) {
            return "job-" + job.getId().substring(0, 26);
        } else {
            return "job-" + job.getId();
        }
    }

    private static String jobExecutionName(BulkImportJob job) {
        String tableName = job.getTableName();
        String jobId = job.getId();
        // See maximum length restriction in AWS documentation:
        // https://docs.aws.amazon.com/step-functions/latest/apireference/API_StartExecution.html#API_StartExecution_RequestParameters
        int spaceForTableName = 80 - jobId.length() - 1;
        if (tableName.length() > spaceForTableName) {
            tableName = tableName.substring(0, spaceForTableName);
        }
        return String.join("-", tableName, jobId);
    }
}
