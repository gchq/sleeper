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

package sleeper.configuration.properties.instance;

import sleeper.configuration.Utils;
import sleeper.configuration.properties.SleeperPropertyIndex;

import java.util.List;

public interface AthenaProperty {
    UserDefinedInstanceProperty SPILL_BUCKET_AGE_OFF_IN_DAYS = Index.propertyBuilder("sleeper.athena.spill.bucket.ageoff.days")
            .description("The number of days before objects in the spill bucket are deleted.")
            .defaultValue("1")
            .propertyGroup(InstancePropertyGroup.ATHENA)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty ATHENA_COMPOSITE_HANDLER_CLASSES = Index.propertyBuilder("sleeper.athena.handler.classes")
            .description("The fully qualified composite classes to deploy. These are the classes that interact with Athena. " +
                    "You can choose to remove one if you don't need them. Both are deployed by default.")
            .defaultValue("sleeper.athena.composite.SimpleCompositeHandler,sleeper.athena.composite.IteratorApplyingCompositeHandler")
            .propertyGroup(InstancePropertyGroup.ATHENA)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty ATHENA_COMPOSITE_HANDLER_MEMORY = Index.propertyBuilder("sleeper.athena.handler.memory")
            .description("The amount of memory (MB) the athena composite handler has.")
            .defaultValue("4096")
            .propertyGroup(InstancePropertyGroup.ATHENA)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty ATHENA_COMPOSITE_HANDLER_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.athena.handler.timeout.seconds")
            .description("The timeout in seconds for the athena composite handler.")
            .defaultValue("900")
            .validationPredicate(Utils::isValidLambdaTimeout)
            .propertyGroup(InstancePropertyGroup.ATHENA)
            .runCdkDeployWhenChanged(true).build();

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
