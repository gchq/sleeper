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
package sleeper.bulkexport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.sqs.AmazonSQS;

import sleeper.bulkexport.model.BulkExportQueryOrLeafPartitionQuery;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.util.ObjectFactoryException;

/**
 * Lambda to start the bulk export job.
 */
public class SqsBulkExportProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsBulkExportProcessor.class);

    private final InstanceProperties instanceProperties;
    private final AmazonSQS sqsClient;
    private final TablePropertiesProvider tablePropertiesProvider;

    private SqsBulkExportProcessor(Builder builder) throws ObjectFactoryException {
        sqsClient = builder.sqsClient;
        instanceProperties = builder.instanceProperties;
        tablePropertiesProvider = builder.tablePropertiesProvider;
    }

    public void processExport(BulkExportQueryOrLeafPartitionQuery exportQuery) {
        throw new RuntimeException("Not implemented");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private AmazonSQS sqsClient;
        private InstanceProperties instanceProperties;
        private TablePropertiesProvider tablePropertiesProvider;

        private Builder() {
        }

        public Builder sqsClient(AmazonSQS sqsClient) {
            this.sqsClient = sqsClient;
            return this;
        }

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        public Builder tablePropertiesProvider(TablePropertiesProvider tablePropertiesProvider) {
            this.tablePropertiesProvider = tablePropertiesProvider;
            return this;
        }

        public SqsBulkExportProcessor build() throws ObjectFactoryException {
            return new SqsBulkExportProcessor(this);
        }
    }
}
