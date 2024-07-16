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
package sleeper.systemtest.drivers.cdk;

import java.util.List;
import java.util.stream.Collectors;

public class TearDownInstancesException extends RuntimeException {

    public TearDownInstancesException(List<TearDownFailure> tearDownFailures) {
        super("Failed to tear down instances: " +
                tearDownFailures.stream().map(TearDownFailure::getInstanceId).collect(Collectors.toList()),
                tearDownFailures.stream().map(TearDownFailure::getFailure).findFirst().orElse(null));
    }

    public static class TearDownFailure {
        private String instanceId;
        private Exception failure;

        public TearDownFailure(String instanceId, Exception failure) {
            this.instanceId = instanceId;
            this.failure = failure;
        }

        public String getInstanceId() {
            return instanceId;
        }

        public Exception getFailure() {
            return failure;
        }
    }

}
