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

/**
 * Definitions of instance properties relating to bulk import.
 */
public interface BulkImportProperty {
    UserDefinedInstanceProperty BULK_IMPORT_CLASS_NAME = Index.propertyBuilder("sleeper.bulk.import.class.name")
            .description("The class to use to perform the bulk import. The default value below uses Spark Dataframes. There is an " +
                    "alternative option that uses RDDs (sleeper.bulkimport.runner.rdd.BulkImportJobRDDDriver).")
            .defaultValue("sleeper.bulkimport.runner.dataframelocalsort.BulkImportDataframeLocalSortDriver")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_SPARK_SHUFFLE_MAPSTATUS_COMPRESSION_CODEC = Index.propertyBuilder("sleeper.bulk.import.emr.spark.shuffle.mapStatus.compression.codec")
            .description("The compression codec for map status results. Used to set spark.shuffle.mapStatus.compression.codec.\n" +
                    "Stops \"Decompression error: Version not supported\" errors - only a value of \"lz4\" has been tested.")
            .defaultValue("lz4")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_SPARK_SPECULATION = Index.propertyBuilder("sleeper.bulk.import.emr.spark.speculation")
            .description("If true then speculative execution of tasks will be performed. Used to set spark.speculation.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("false")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_SPARK_SPECULATION_QUANTILE = Index.propertyBuilder("sleeper.bulk.import.spark.speculation.quantile")
            .description("Fraction of tasks which must be complete before speculation is enabled for a particular stage. Used to set spark.speculation.quantile.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("0.75")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_STARTER_LAMBDA_MEMORY = Index.propertyBuilder("sleeper.bulk.import.starter.memory.mb")
            .description("The amount of memory in MB for lambda functions that start bulk import jobs.")
            .defaultProperty(TableStateProperty.DEFAULT_TABLE_STATE_LAMBDA_MEMORY)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
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
