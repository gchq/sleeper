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
package sleeper.core.properties.model;

/**
 * Helpers to generate the name of AWS resources containing Sleeper deployment artefacts.
 */
public class SleeperArtefactsLocation {

    private SleeperArtefactsLocation() {
    }

    /**
     * Computes the default jars bucket name from the instance ID. See the instance property `sleeper.jars.bucket`.
     *
     * @param  instanceId the instance ID
     * @return            the bucket name
     */
    public static String getDefaultJarsBucketName(String instanceId) {
        return "sleeper-" + instanceId + "-jars";
    }

    /**
     * Computes the default ECR repository prefix from the instance ID. See the instance property
     * `sleeper.ecr.repository.prefix`.
     *
     * @param  instanceId the instance ID
     * @return            the repository prefix
     */
    public static String getDefaultEcrRepositoryPrefix(String instanceId) {
        return instanceId;
    }

}
