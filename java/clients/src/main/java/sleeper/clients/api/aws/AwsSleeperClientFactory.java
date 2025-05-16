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
package sleeper.clients.api.aws;

import sleeper.clients.api.SleeperClient;
import sleeper.clients.api.SleeperClientFactory;
import sleeper.core.properties.instance.InstanceProperties;

public class AwsSleeperClientFactory implements SleeperClientFactory {

    private final AwsSleeperClientBuilder builder;

    private AwsSleeperClientFactory(AwsSleeperClientBuilder builder) {
        this.builder = builder;
    }

    @Override
    public SleeperClient createClientForInstance(String instanceId) {
        return builder.instanceId(instanceId).build();
    }

    @Override
    public SleeperClient createClientForInstance(InstanceProperties instanceProperties) {
        return builder.instanceProperties(instanceProperties).build();
    }

}
