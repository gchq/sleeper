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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import sleeper.core.properties.SleeperPropertyIndex;

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

    /**
     * An index of definitions of all user-defined instance properties.
     */
    class Index {
        private Index() {
        }

        private static final SleeperPropertyIndex<UserDefinedInstanceProperty> INSTANCE = createInstance();

        private static SleeperPropertyIndex<UserDefinedInstanceProperty> createInstance() {
            SleeperPropertyIndex<UserDefinedInstanceProperty> index = new SleeperPropertyIndex<>();
            index.addAll(CommonProperty.getAll());
            index.addAll(TableStateProperty.getAll());
            index.addAll(IngestProperty.getAll());
            index.addAll(ArrayListIngestProperty.getAll());
            index.addAll(ArrowIngestProperty.getAll());
            index.addAll(AsyncIngestPartitionFileWriterProperty.getAll());
            index.addAll(BatcherProperty.getAll());
            index.addAll(BulkImportProperty.getAll());
            index.addAll(EMRProperty.getAll());
            index.addAll(EMRServerlessProperty.getAll());
            index.addAll(NonPersistentEMRProperty.getAll());
            index.addAll(PersistentEMRProperty.getAll());
            index.addAll(EKSProperty.getAll());
            index.addAll(PartitionSplittingProperty.getAll());
            index.addAll(GarbageCollectionProperty.getAll());
            index.addAll(CompactionProperty.getAll());
            index.addAll(QueryProperty.getAll());
            index.addAll(MetricsProperty.getAll());
            index.addAll(LoggingLevelsProperty.getAll());
            index.addAll(AthenaProperty.getAll());
            index.addAll(DefaultProperty.getAll());

            return index;
        }
    }
}
