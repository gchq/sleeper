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
 * Definitions of instance properties relating to the ingest batcher.
 */
public interface BatcherProperty {
    UserDefinedInstanceProperty INGEST_BATCHER_SUBMITTER_MEMORY_IN_MB = Index.propertyBuilder("sleeper.ingest.batcher.submitter.memory.mb")
            .description("The amount of memory in MB for the lambda that receives submitted requests to ingest files.")
            .defaultValue("1024")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty INGEST_BATCHER_SUBMITTER_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.ingest.batcher.submitter.timeout.seconds")
            .description("The timeout in seconds for the lambda that receives submitted requests to ingest files.")
            .defaultValue("20")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty INGEST_BATCHER_JOB_CREATION_MEMORY_IN_MB = Index.propertyBuilder("sleeper.ingest.batcher.job.creation.memory.mb")
            .description("The amount of memory in MB for the lambda that creates ingest jobs from submitted file ingest requests.")
            .defaultValue("1024")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty INGEST_BATCHER_JOB_CREATION_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.ingest.batcher.job.creation.timeout.seconds")
            .description("The timeout in seconds for the lambda that creates ingest jobs from submitted file ingest requests.")
            .defaultValue("900")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty INGEST_BATCHER_JOB_CREATION_LAMBDA_PERIOD_IN_MINUTES = Index.propertyBuilder("sleeper.ingest.batcher.job.creation.period.minutes")
            .description("The rate at which the ingest batcher job creation lambda runs (in minutes, must be >=1).")
            .defaultValue("1")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.INGEST)
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
