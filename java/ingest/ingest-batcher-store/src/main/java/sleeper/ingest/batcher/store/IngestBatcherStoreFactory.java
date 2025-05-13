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

package sleeper.ingest.batcher.store;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.OptionalStack;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.ingest.batcher.core.IngestBatcherStore;

import java.util.Optional;

import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;

public class IngestBatcherStoreFactory {
    private IngestBatcherStoreFactory() {
    }

    public static Optional<IngestBatcherStore> getStore(
            AmazonDynamoDB dynamoDB, InstanceProperties properties, TablePropertiesProvider tablePropertiesProvider) {
        if (properties.getEnumList(OPTIONAL_STACKS, OptionalStack.class).contains(OptionalStack.IngestBatcherStack)) {
            return Optional.of(new DynamoDBIngestBatcherStore(dynamoDB, properties, tablePropertiesProvider));
        }
        return Optional.empty();
    }
}
