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
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final AmazonElasticMapReduce emrClient;
    private final AWSStepFunctions stepFunctionsClient;
    private final String bulkImportPlatform;
    private final String taskId;
    private final Supplier<Instant> validationTimeSupplier;

    public ExecutorFactory(AmazonS3 s3Client,
                           AmazonElasticMapReduce emrClient,
                           AWSStepFunctions stepFunctionsClient,
                           AmazonDynamoDB dynamoDB,
                           String taskId,
                           Supplier<Instant> validationTimeSupplier) throws IOException {
        this(s3Client, emrClient, stepFunctionsClient, dynamoDB, taskId, validationTimeSupplier, System::getenv);
    }

    ExecutorFactory(AmazonS3 s3Client,
                    AmazonElasticMapReduce emrClient,
                    AWSStepFunctions stepFunctionsClient,
                    AmazonDynamoDB dynamoDB,
                    String taskId,
                    Supplier<Instant> validationTimeSupplier,
                    UnaryOperator<String> getEnvironmentVariable) throws IOException {
        this.instanceProperties = new InstanceProperties();
        this.instanceProperties.loadFromS3(s3Client, getEnvironmentVariable.apply(CONFIG_BUCKET.toEnvironmentVariable()));
        this.tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
        this.stateStoreProvider = new StateStoreProvider(dynamoDB, instanceProperties);
        this.s3Client = s3Client;
        this.emrClient = emrClient;
        this.stepFunctionsClient = stepFunctionsClient;
        this.bulkImportPlatform = getEnvironmentVariable.apply(BULK_IMPORT_PLATFORM);
        this.ingestJobStatusStore = new DynamoDBIngestJobStatusStore(dynamoDB, instanceProperties);
        this.taskId = taskId;
        this.validationTimeSupplier = validationTimeSupplier;
        LOGGER.info("Initialised ExecutorFactory. Environment variable {} is set to {}.", BULK_IMPORT_PLATFORM, this.bulkImportPlatform);
    }

    public Executor createExecutor() {
        switch (bulkImportPlatform) {
            case "NonPersistentEMR":
                return new EmrExecutor(emrClient, instanceProperties, tablePropertiesProvider,
                        stateStoreProvider, ingestJobStatusStore, s3Client, taskId, validationTimeSupplier);
            case "EKS":
                return new StateMachineExecutor(stepFunctionsClient, instanceProperties, tablePropertiesProvider,
                        stateStoreProvider, ingestJobStatusStore, s3Client, taskId, validationTimeSupplier);
            case "PersistentEMR":
                return new PersistentEmrExecutor(emrClient, instanceProperties, tablePropertiesProvider,
                        stateStoreProvider, ingestJobStatusStore, s3Client, taskId, validationTimeSupplier);
            default:
                throw new IllegalArgumentException("Invalid value for " + System.getenv(BULK_IMPORT_PLATFORM));
        }
    }
}
