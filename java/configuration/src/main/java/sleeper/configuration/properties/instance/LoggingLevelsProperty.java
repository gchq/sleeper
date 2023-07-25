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

package sleeper.configuration.properties.instance;


import sleeper.configuration.properties.SleeperPropertyIndex;

import java.util.List;

public interface LoggingLevelsProperty {
    UserDefinedInstanceProperty LOGGING_LEVEL = Index.propertyBuilder("sleeper.logging.level")
            .description("The logging level for logging Sleeper classes. This does not apply to the MetricsLogger which is always set to INFO.")
            .propertyGroup(InstancePropertyGroup.LOGGING)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty APACHE_LOGGING_LEVEL = Index.propertyBuilder("sleeper.logging.apache.level")
            .description("The logging level for Apache logs that are not Parquet.")
            .propertyGroup(InstancePropertyGroup.LOGGING)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty PARQUET_LOGGING_LEVEL = Index.propertyBuilder("sleeper.logging.parquet.level")
            .description("The logging level for Parquet logs.")
            .propertyGroup(InstancePropertyGroup.LOGGING)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty AWS_LOGGING_LEVEL = Index.propertyBuilder("sleeper.logging.aws.level")
            .description("The logging level for AWS logs.")
            .propertyGroup(InstancePropertyGroup.LOGGING)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty ROOT_LOGGING_LEVEL = Index.propertyBuilder("sleeper.logging.root.level")
            .description("The logging level for everything else.")
            .propertyGroup(InstancePropertyGroup.LOGGING)
            .runCDKDeployWhenChanged(true).build();

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
