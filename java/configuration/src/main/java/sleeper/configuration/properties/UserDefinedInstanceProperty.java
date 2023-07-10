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
package sleeper.configuration.properties;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.List;

/**
 * Sleeper properties set by the user. All non-mandatory properties should be accompanied by a default value and should
 * have a validation predicate for determining if the value a user has provided is valid. By default, the predicate
 * always returns true indicating the property is valid.
 */
// Suppress as this class will always be referenced before impl class, so initialization behaviour will be deterministic
@SuppressFBWarnings("IC_SUPERCLASS_USES_SUBCLASS_DURING_INITIALIZATION")
public interface UserDefinedInstanceProperty extends InstanceProperty {

    static List<UserDefinedInstanceProperty> getAll() {
        return Index.INSTANCE.getAll();
    }

    static boolean has(String propertyName) {
        return Index.INSTANCE.getByName(propertyName).isPresent();
    }

    class Index {
        private Index() {
        }

        private static final SleeperPropertyIndex<UserDefinedInstanceProperty> INSTANCE = createInstance();

        private static SleeperPropertyIndex<UserDefinedInstanceProperty> createInstance() {
            SleeperPropertyIndex<UserDefinedInstanceProperty> index = new SleeperPropertyIndex<>();
            index.addAll(ArrayListIngestProperties.getAll());
            index.addAll(ArrowIngestProperties.getAll());
            index.addAll(AsyncIngestPartitionFileWriterProperties.getAll());
            index.addAll(AthenaProperties.getAll());
            index.addAll(BatcherProperties.getAll());
            index.addAll(BulkImportProperties.getAll());
            index.addAll(BulkImportUsingEKSProperties.getAll());
            index.addAll(BulkImportUsingEMRProperties.getAll());
            index.addAll(BulkImportUsingEMRPropertiesNonPersistant.getAll());
            index.addAll(BulkImportUsingEMRPropertiesPersistent.getAll());
            index.addAll(CompactionProperties.getAll());
            index.addAll(DashboardProperties.getAll());
            index.addAll(CommonProperties.getAll());
            index.addAll(GarbageCollectionProperties.getAll());
            index.addAll(IngestProperties.getAll());
            index.addAll(LoggingLevelsProperties.getAll());
            index.addAll(PartitionSplittingProperties.getAll());
            index.addAll(QueryProperties.getAll());
            index.addAll(StatusStoreProperties.getAll());




            return index;
        }


    }
}
