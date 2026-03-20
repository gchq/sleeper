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
package sleeper.systemtest.configuration;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.SleeperArtefactsLocation;

import static sleeper.core.properties.instance.CommonProperty.ARTEFACTS_DEPLOYMENT_ID;

public class SystemTestDockerRepository {

    private SystemTestDockerRepository() {
    }

    public static String getRepositoryName(InstanceProperties instanceProperties) {
        return SleeperArtefactsLocation.getDefaultEcrRepositoryPrefix(instanceProperties.get(ARTEFACTS_DEPLOYMENT_ID)) + "/system-test";
    }

}
