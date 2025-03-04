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
package sleeper.core.properties.instance;

import sleeper.core.properties.SleeperPropertyIndex;
import sleeper.core.properties.validation.SleeperPropertyValueUtils;

import java.util.List;

import static sleeper.core.properties.instance.TableStateProperty.DEFAULT_TABLE_STATE_LAMBDA_MEMORY;

/**
 * Properties for bulk export processing.
 */
public interface BulkExportProperty {
    UserDefinedInstanceProperty BULK_EXPORT_LAMBDA_MEMORY_IN_MB = Index
            .propertyBuilder("sleeper.bulk.export.memory.mb")
            .description("The amount of memory in MB for lambda functions that start bulk export jobs.")
            .defaultProperty(DEFAULT_TABLE_STATE_LAMBDA_MEMORY)
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_EXPORT_LAMBDA_TIMEOUT_IN_SECONDS = Index
            .propertyBuilder("sleeper.bulk.export.timeout.seconds")
            .description("The default timeout in seconds for the bulk export lambda.")
            .defaultValue("800")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_EXPORT_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS = Index
            .propertyBuilder("sleeper.bulk.export.queue.visibility.timeout.seconds")
            .description("The visibility timeout in seconds for the bulk export queue.")
            .defaultValue("800")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_EXPORT_LEAF_PARTITION_LANGUAGE = Index
            .propertyBuilder("sleeper.bulk.export.leaf.partition.language")
            .description("The language to use for the leaf partition bulk export processor.")
            .defaultValue("java")
            .validationPredicate(SleeperPropertyValueUtils::isValidBulkExportLanguage)
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .runCdkDeployWhenChanged(true).build();

    static List<UserDefinedInstanceProperty> getAll() {
        return Index.INSTANCE.getAll();
    }

    /**
     * An index of property definitions in this file.
     */
    class Index {
        private Index() {
        }

        private static final SleeperPropertyIndex<UserDefinedInstanceProperty> INSTANCE = new SleeperPropertyIndex<>();

        private static UserDefinedInstancePropertyImpl.Builder propertyBuilder(String propertyName) {
            return UserDefinedInstancePropertyImpl.named(propertyName)
                    .addToIndex(INSTANCE::add);
        }
    }

}
