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
package sleeper.bulkexport.plannerV2;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

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
import sleeper.statestorev2.StateStoreFactory;

import java.util.List;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.LEAF_PARTITION_BULK_EXPORT_QUEUE_URL;

/**
 * Lambda to start the bulk export job.
 */
public class SqsBulkExportProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsBulkExportProcessor.class);

    private final InstanceProperties instanceProperties;
    private final SqsClient sqsClient;
    private final S3Client s3Client;
    private final DynamoDbClient dynamoClient;
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
     * @param  bulkExportQuery        the bulk export query to be processed
     * @throws ObjectFactoryException if there is an error creating the necessary objects
     */
    public void processExport(BulkExportQuery bulkExportQuery) throws ObjectFactoryException {
        String sqsUrl = instanceProperties.get(LEAF_PARTITION_BULK_EXPORT_QUEUE_URL);
        TableProperties tableProperties = bulkExportQuery.getTableProperties(tablePropertiesProvider);
        StateStore statestore = stateStoreProvider.getStateStore(tableProperties);
        BulkExportQuerySplitter splitter = new BulkExportQuerySplitter(tableProperties, statestore);
        BulkExportLeafPartitionQuerySerDe querySerDe = new BulkExportLeafPartitionQuerySerDe(tablePropertiesProvider);
        splitter.initIfNeeded();
        List<BulkExportLeafPartitionQuery> leafPartitionQueries = splitter.splitIntoLeafPartitionQueries(bulkExportQuery);
        LOGGER.debug("Got {} leaf partition export queries for bulk export query {}.",
                leafPartitionQueries.size(), bulkExportQuery.getExportId());
        leafPartitionQueries.forEach(query -> {
            LOGGER.debug("Sending leaf partition export query {} to queue {}.", query.getSubExportId(), sqsUrl);
            SendMessageRequest request = SendMessageRequest.builder().queueUrl(sqsUrl)
                    .messageBody(querySerDe.toJson(query)).build();
            sqsClient.sendMessage(request);
        });
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder to create a bulk export processor.
     *
     * <p>Example usage:</p>
     *
     * <pre>{@code
     * SqsBulkExportProcessor processor = SqsBulkExportProcessor.builder()
     *         .sqsClient(sqsClient)
     *         .s3Client(s3Client)
     *         .dynamoClient(dynamoClient)
     *         .instanceProperties(instanceProperties)
     *         .tablePropertiesProvider(tablePropertiesProvider)
     *         .build();
     * }</pre>
     */
    public static final class Builder {
        private SqsClient sqsClient;
        private S3Client s3Client;
        private DynamoDbClient dynamoClient;
        private InstanceProperties instanceProperties;
        private TablePropertiesProvider tablePropertiesProvider;

        private Builder() {
        }

        /**
         * Sets the S3 client.
         *
         * @param  s3Client the S3 client
         * @return          the builder for method chaining
         */
        public Builder s3Client(S3Client s3Client) {
            this.s3Client = s3Client;
            return this;
        }

        /**
         * Sets the SQS client.
         *
         * @param  sqsClient the SQS client
         * @return           the builder for method chaining
         */
        public Builder sqsClient(SqsClient sqsClient) {
            this.sqsClient = sqsClient;
            return this;
        }

        /**
         * Sets the DynamoDB client.
         *
         * @param  dynamoClient the DynamoDB client
         * @return              the builder for method chaining
         */
        public Builder dynamoClient(DynamoDbClient dynamoClient) {
            this.dynamoClient = dynamoClient;
            return this;
        }

        /**
         * Sets the instance properties.
         *
         * @param  instanceProperties the instance properties
         * @return                    the builder for method chaining
         */
        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        /**
         * Sets the table properties provider.
         *
         * @param  tablePropertiesProvider the table properties provider
         * @return                         the builder for method chaining
         */
        public Builder tablePropertiesProvider(TablePropertiesProvider tablePropertiesProvider) {
            this.tablePropertiesProvider = tablePropertiesProvider;
            return this;
        }

        public SqsBulkExportProcessor build() throws ObjectFactoryException {
            return new SqsBulkExportProcessor(this);
        }
    }
}
