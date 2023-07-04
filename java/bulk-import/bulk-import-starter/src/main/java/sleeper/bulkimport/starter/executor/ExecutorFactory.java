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
package sleeper.bulkimport.starter.executor;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.job.DynamoDBIngestJobStatusStore;
import sleeper.statestore.StateStoreProvider;

import java.io.IOException;
import java.time.Instant;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;

public class ExecutorFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorFactory.class);
    private static final String BULK_IMPORT_PLATFORM = "BULK_IMPORT_PLATFORM";

    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final IngestJobStatusStore ingestJobStatusStore;
    private final AmazonS3 s3Client;
    private AmazonElasticMapReduce emrClient;
    private EmrServerlessClient emrServerlessClient;
    private final AWSStepFunctions stepFunctionsClient;
    private final String bulkImportPlatform;
    private final Supplier<Instant> validationTimeSupplier;

    public ExecutorFactory(InstanceProperties instanceProperties, AmazonS3 s3Client,
                           AmazonElasticMapReduce emrClient,
                           AWSStepFunctions stepFunctionsClient,
                           AmazonDynamoDB dynamoDB) {
        this(instanceProperties, s3Client, emrClient, stepFunctionsClient, dynamoDB, Instant::now, System::getenv);
        this.emrClient = emrClient;
        logSetup();
    }

    @VisibleForTesting
    public ExecutorFactory(AmazonS3 s3Client,
                           AmazonElasticMapReduce emrClient,
                           AWSStepFunctions stepFunctionsClient,
                           AmazonDynamoDB dynamoDB,
                           Supplier<Instant> validationTimeSupplier,
                           UnaryOperator<String> environmentVariable) throws IOException {
        this(s3Client, stepFunctionsClient, dynamoDB, validationTimeSupplier, environmentVariable);
        this.emrClient = emrClient;
        logSetup();
    }

    public ExecutorFactory(AmazonS3 s3Client,
                           EmrServerlessClient emrServerlessClient,
                           AWSStepFunctions stepFunctionsClient,
                           AmazonDynamoDB dynamoDB) throws IOException {
        this(s3Client, stepFunctionsClient, dynamoDB, Instant::now, System::getenv);
        this.emrServerlessClient = emrServerlessClient;
        logSetup();
    }

    @VisibleForTesting
    public ExecutorFactory(AmazonS3 s3Client,
                           EmrServerlessClient emrServerlessClient,
                           AWSStepFunctions stepFunctionsClient,
                           AmazonDynamoDB dynamoDB,
                           Supplier<Instant> validationTimeSupplier,
                           UnaryOperator<String> environmentVariable) throws IOException {
        this(s3Client, stepFunctionsClient, dynamoDB, validationTimeSupplier, environmentVariable);
        this.emrServerlessClient = emrServerlessClient;
        logSetup();
    }

    ExecutorFactory(AmazonS3 s3Client,
                    AWSStepFunctions stepFunctionsClient,
                    AmazonDynamoDB dynamoDB,
                    Supplier<Instant> validationTimeSupplier,
                    UnaryOperator<String> getEnvironmentVariable) throws IOException {
        this(loadInstanceProperties(s3Client, getEnvironmentVariable), s3Client, AmazonElasticMapReduceClientBuilder.defaultClient(),
            stepFunctionsClient, dynamoDB, validationTimeSupplier, getEnvironmentVariable);
    }

    ExecutorFactory(InstanceProperties instanceProperties, AmazonS3 s3Client,
                    AmazonElasticMapReduce emrClient,
                    AWSStepFunctions stepFunctionsClient,
                    AmazonDynamoDB dynamoDB,
                    Supplier<Instant> validationTimeSupplier,
                    UnaryOperator<String> getEnvironmentVariable) {
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
        this.stateStoreProvider = new StateStoreProvider(dynamoDB, instanceProperties);
        this.s3Client = s3Client;
        this.stepFunctionsClient = stepFunctionsClient;
        this.bulkImportPlatform = getEnvironmentVariable.apply(BULK_IMPORT_PLATFORM);
        this.ingestJobStatusStore = new DynamoDBIngestJobStatusStore(dynamoDB, instanceProperties);
        this.validationTimeSupplier = validationTimeSupplier;
        logSetup();
    }

    private void logSetup() {
        LOGGER.info("Initialised ExecutorFactory. Environment variable {} is set to {}.", BULK_IMPORT_PLATFORM, this.bulkImportPlatform);
    }

    private static InstanceProperties loadInstanceProperties(AmazonS3 s3Client, UnaryOperator<String> getEnvironmentVariable) throws IOException {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3Client, getEnvironmentVariable.apply(CONFIG_BUCKET.toEnvironmentVariable()));
        return instanceProperties;
    }

    public Executor createExecutor() {
        switch (bulkImportPlatform) {
            case "NonPersistentEMR":
                return new EmrExecutor(emrClient, instanceProperties, tablePropertiesProvider,
                        stateStoreProvider, ingestJobStatusStore, s3Client, validationTimeSupplier);
            case "EMRServerless":
                return new EmrServerlessExecutor(emrServerlessClient, instanceProperties, tablePropertiesProvider,
                stateStoreProvider, ingestJobStatusStore, s3Client, validationTimeSupplier);
            case "EKS":
                return new StateMachineExecutor(stepFunctionsClient, instanceProperties, tablePropertiesProvider,
                        stateStoreProvider, ingestJobStatusStore, s3Client, validationTimeSupplier);
            case "PersistentEMR":
                return new PersistentEmrExecutor(emrClient, instanceProperties, tablePropertiesProvider,
                        stateStoreProvider, ingestJobStatusStore, s3Client, validationTimeSupplier);
            default:
                throw new IllegalArgumentException("Invalid value for " + System.getenv(BULK_IMPORT_PLATFORM));
        }
    }
}
