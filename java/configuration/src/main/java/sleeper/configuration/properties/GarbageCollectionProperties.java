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

import java.util.List;

public interface GarbageCollectionProperties {
    UserDefinedInstanceProperty GARBAGE_COLLECTOR_PERIOD_IN_MINUTES = Index.propertyBuilder("sleeper.gc.period.minutes")
            .description("The frequency in minutes with which the garbage collector lambda is run.")
            .defaultValue("15")
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty GARBAGE_COLLECTOR_LAMBDA_MEMORY_IN_MB = Index.propertyBuilder("sleeper.gc.memory")
            .description("The memory in MB for the lambda function used to perform garbage collection.")
            .defaultValue("1024")
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty GARBAGE_COLLECTOR_BATCH_SIZE = Index.propertyBuilder("sleeper.gc.batch.size")
            .description("The size of the batch of files ready for garbage collection requested from the State Store.")
            .defaultValue("2000")
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR).build();
    UserDefinedInstanceProperty DEFAULT_GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION = Index.propertyBuilder("sleeper.default.gc.delay.minutes")
            .description("A file will not be deleted until this number of minutes have passed after it has been marked as ready for " +
                    "garbage collection. The reason for not deleting files immediately after they have been marked as ready for " +
                    "garbage collection is that they may still be in use by queries. This property can be overridden on a per-table " +
                    "basis.")
            .defaultValue("15")
            .propertyGroup(InstancePropertyGroup.GARBAGE_COLLECTOR).build();

    static List<UserDefinedInstanceProperty> getAll() {
        return Index.INSTANCE.getAll();
    }

    static boolean has(String propertyName) {
        return Index.INSTANCE.getByName(propertyName).isPresent();
    }

    class Index {
        private Index() {
        }

        private static final SleeperPropertyIndex<UserDefinedInstanceProperty> INSTANCE = new SleeperPropertyIndex<>();

        static UserDefinedInstancePropertyImpl.Builder propertyBuilder(String propertyName) {
            return UserDefinedInstancePropertyImpl.named(propertyName)
                    .addToIndex(INSTANCE::add);
        }
    }
}
