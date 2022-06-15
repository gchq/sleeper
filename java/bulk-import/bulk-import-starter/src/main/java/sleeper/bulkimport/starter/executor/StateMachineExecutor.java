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
package sleeper.bulkimport.starter.executor;

import com.amazonaws.auth.WebIdentityTokenCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.model.StartExecutionRequest;
import com.google.gson.Gson;
import sleeper.bulkimport.job.BulkImportJob;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperty;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_EKS_CLUSTER_ENDPOINT;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_EKS_NAMESPACE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_EKS_STATE_MACHINE_ARN;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ACCOUNT;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_REPO;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAXIMUM_CONNECTIONS_TO_S3;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.REGION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VERSION;

/**
 * A {@link StateMachineExecutor} Generates the arguments and configuration to
 * run a job using spark on an EKS cluster. It creates a list of arguments and
 * submits them to a state machine in AWS Step Functions.
 */
public class StateMachineExecutor extends Executor {
    private static final String DEFAULT_JAR_LOCATION = "local:///opt/spark/workdir/bulk-import-runner.jar";
    private static final String DEFAULT_LOG4J_LOCATION = "file:///opt/spark/workdir/log4j.properties";
    private static final Map<String, String> DEFAULT_CONFIG;

    private final AWSStepFunctions stepFunctions;

    static {
        Map<String, String> defaultConf = new HashMap<>();
        defaultConf.put("spark.executor.instances", "3");
        // Default Memory requests are overwritten because Fargate doesn't work with
        // spark's default values
        defaultConf.put("spark.driver.memory", "7g");
        defaultConf.put("spark.executor.memory", "7g");
        // Fargate provides extra memory so no need to include extra which also messes
        // up the scheduler
        defaultConf.put("spark.driver.memoryOverhead", "1g");
        defaultConf.put("spark.executor.memoryOverhead", "1g");
        defaultConf.put("spark.driver.extraJavaOptions", "-Dlog4j.configuration=" + DEFAULT_LOG4J_LOCATION);
        defaultConf.put("spark.executor.extraJavaOptions", "-Dlog4j.configuration=" + DEFAULT_LOG4J_LOCATION);
        defaultConf.put("spark.kubernetes.authenticate.driver.serviceAccountName", "spark");
        // Hadoop Configuration
        defaultConf.put("spark.hadoop.fs.s3a.aws.credentials.provider",
                WebIdentityTokenCredentialsProvider.class.getName());
        defaultConf.put("spark.hadoop.fs.s3a.experimental.input.fadvise", "sequential");
        DEFAULT_CONFIG = Collections.unmodifiableMap(defaultConf);
    }

    public StateMachineExecutor(AWSStepFunctions stepFunctions,
                                InstanceProperties instanceProperties,
                                TablePropertiesProvider tablePropertiesProvider,
                                AmazonS3 s3Client) {
        super(instanceProperties, tablePropertiesProvider, s3Client);
        this.stepFunctions = stepFunctions;
    }

    @Override
    public void runJobOnPlatform(BulkImportJob bulkImportJob) {
        Map<String, Object> input = new HashMap<>();
        List<String> args = constructArgs(bulkImportJob);
        input.put("job", bulkImportJob);
        input.put("args", args);

        stepFunctions.startExecution(
                new StartExecutionRequest()
                        .withStateMachineArn(getInstanceProperties().get(BULK_IMPORT_EKS_STATE_MACHINE_ARN))
                        .withName(String.join("-", "sleeper", getInstanceProperties().get(ID), bulkImportJob.getTableName(), bulkImportJob.getId()))
                        .withInput(new Gson().toJson(input)));
    }

    @Override
    protected Map<String, String> getDefaultSparkConfig(BulkImportJob bulkImportJob, Map<String, String> platformSpec, TableProperties tableProperties) {
        InstanceProperties instanceProperties = getInstanceProperties();
        Map<String, String> defaultConfig = new HashMap<>(DEFAULT_CONFIG);
        String imageName = instanceProperties.get(ACCOUNT) + ".dkr.ecr." +
                instanceProperties.get(REGION) + ".amazonaws.com/" +
                instanceProperties.get(BULK_IMPORT_REPO) + ":" + instanceProperties.get(VERSION);
        defaultConfig.put("spark.master", "k8s://" + instanceProperties.get(BULK_IMPORT_EKS_CLUSTER_ENDPOINT));
        defaultConfig.put("spark.app.name", bulkImportJob.getId());
        defaultConfig.put("spark.kubernetes.container.image", imageName);
        defaultConfig.put("spark.kubernetes.namespace", instanceProperties.get(BULK_IMPORT_EKS_NAMESPACE));
        defaultConfig.put("spark.kubernetes.driver.pod.name", bulkImportJob.getId());
        defaultConfig.put("spark.hadoop.fs.s3a.connection.maximum", instanceProperties.get(MAXIMUM_CONNECTIONS_TO_S3));
        defaultConfig.put("spark.shuffle.mapStatus.compression.codec", getFromPlatformSpec(TableProperty.BULK_IMPORT_SPARK_SHUFFLE_MAPSTATUS_COMPRESSION_CODEC, platformSpec, tableProperties));
        defaultConfig.put("spark.speculation", getFromPlatformSpec(TableProperty.BULK_IMPORT_SPARK_SPECULATION, platformSpec, tableProperties));
        defaultConfig.put("spark.speculation.quantile", getFromPlatformSpec(TableProperty.BULK_IMPORT_SPARK_SPECULATION_QUANTILE, platformSpec, tableProperties));

        return defaultConfig;
    }

    @Override
    protected String getJarLocation() {
        return DEFAULT_JAR_LOCATION;
    }
}
