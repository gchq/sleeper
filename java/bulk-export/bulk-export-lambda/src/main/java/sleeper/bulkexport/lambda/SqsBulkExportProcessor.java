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
package sleeper.bulkexport.lambda;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.sqs.AmazonSQS;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkexport.core.model.BulkExportLeafPartitionQuery;
import sleeper.bulkexport.core.model.BulkExportLeafPartitionQuerySerDe;
import sleeper.bulkexport.core.model.BulkExportQuery;
import sleeper.bulkexport.core.recordretrieval.BulkExportQuerySplitter;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.util.ObjectFactoryException;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.statestore.StateStoreFactory;

import java.util.List;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_EXPORT_PROCESSOR_QUEUE_URL;


/**
 * Lambda to start the bulk export job.
 */
public class SqsBulkExportProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsBulkExportProcessor.class);

    private final InstanceProperties instanceProperties;
    private final AmazonSQS sqsClient;
    private final AmazonS3 s3Client;
    private final AmazonDynamoDB dynamoClient;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;

    private SqsBulkExportProcessor(Builder builder) throws ObjectFactoryException {
        sqsClient = builder.sqsClient;
        dynamoClient = builder.dynamoClient;
        s3Client = builder.s3Client;
        instanceProperties = builder.instanceProperties;
        tablePropertiesProvider = builder.tablePropertiesProvider;
        Configuration confForStateStore = HadoopConfigurationProvider.getConfigurationForLambdas(instanceProperties);
        stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client,
                dynamoClient, confForStateStore);
    }

    /**
     * Processes a bulk export query by splitting it into leaf partition queries.
     *
     * @param bulkExportQuery The bulk export query to be processed.
     * @throws ObjectFactoryException If there is an error creating the necessary objects.
     */
    public void processExport(BulkExportQuery bulkExportQuery) throws ObjectFactoryException {
        String sqsUrl = instanceProperties.get(BULK_EXPORT_PROCESSOR_QUEUE_URL);
        TableProperties tableProperties = bulkExportQuery.getTableProperties(tablePropertiesProvider);
        StateStore statestore = stateStoreProvider.getStateStore(tableProperties);
        BulkExportQuerySplitter splitter = new BulkExportQuerySplitter(tableProperties, statestore);
        BulkExportLeafPartitionQuerySerDe querySerDe = new BulkExportLeafPartitionQuerySerDe(tablePropertiesProvider);
        splitter.initIfNeeded();
        List<BulkExportLeafPartitionQuery> leafPartitionQueries = splitter.splitIntoLeafPartitionQueries(bulkExportQuery);
        LOGGER.debug("Got {} leaf partition export queries for bulk export query {}.",
            leafPartitionQueries.size(), bulkExportQuery.getExportId());
        leafPartitionQueries.forEach(query ->  {
            LOGGER.debug("Sending leaf partition export query {} to queue {}.", query.getSubExportId(), sqsUrl);
            sqsClient.sendMessage(sqsUrl, querySerDe.toJson(query));
        });
    }

    /**
     * Creates a new instance of the Builder class.
     *
     * @return a new Builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder class for constructing instances of SqsBulkExportProcessor.
     * This builder allows for the configuration of various clients and properties
     * required by the SqsBulkExportProcessor.
     *
     * <p>Example usage:</p>
     * <pre>{@code
     * SqsBulkExportProcessor processor = SqsBulkExportProcessor.builder()
     *     .sqsClient(sqsClient)
     *     .s3Client(s3Client)
     *     .dynamoClient(dynamoClient)
     *     .instanceProperties(instanceProperties)
     *     .tablePropertiesProvider(tablePropertiesProvider)
     *     .build();
     * }</pre>
     */
    public static final class Builder {
        private AmazonSQS sqsClient;
        private AmazonS3 s3Client;
        private AmazonDynamoDB dynamoClient;
        private InstanceProperties instanceProperties;
        private TablePropertiesProvider tablePropertiesProvider;

        private Builder() {
        }

        /**
         * Sets the Amazon S3 client to be used by this builder.
         *
         * @param s3Client the Amazon S3 client to set
         * @return the builder instance with the specified Amazon S3 client
         */
        public Builder s3Client(AmazonS3 s3Client) {
            this.s3Client = s3Client;
            return this;
        }

        /**
         * Sets the AmazonSQS client to be used by the SqsBulkExportProcessor.
         *
         * @param sqsClient the AmazonSQS client to set
         * @return the Builder instance for method chaining
         */
        public Builder sqsClient(AmazonSQS sqsClient) {
            this.sqsClient = sqsClient;
            return this;
        }

        /**
         * Sets the AmazonDynamoDB client to be used by the SqsBulkExportProcessor.
         *
         * @param dynamoClient the AmazonDynamoDB client instance
         * @return the Builder instance for method chaining
         */
        public Builder dynamoClient(AmazonDynamoDB dynamoClient) {
            this.dynamoClient = dynamoClient;
            return this;
        }

        /**
         * Sets the instance properties for the SqsBulkExportProcessor.
         *
         * @param instanceProperties the instance properties to set
         * @return the Builder instance for method chaining
         */
        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        /**
         * Sets the TablePropertiesProvider for this builder.
         *
         * @param tablePropertiesProvider the TablePropertiesProvider to set
         * @return the Builder instance for method chaining
         */
        public Builder tablePropertiesProvider(TablePropertiesProvider tablePropertiesProvider) {
            this.tablePropertiesProvider = tablePropertiesProvider;
            return this;
        }

        /**
         * Builds and returns a new instance of SqsBulkExportProcessor.
         *
         * @return A new instance of {@link SqsBulkExportProcessor}.
         * @throws ObjectFactoryException If there is an error during the creation of the {@link SqsBulkExportProcessor} instance.
         */
        public SqsBulkExportProcessor build() throws ObjectFactoryException {
            return new SqsBulkExportProcessor(this);
        }
    }
}
