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
package sleeper.core.deploy;

import sleeper.core.properties.instance.InstanceProperties;

import java.util.Optional;

import static sleeper.core.properties.instance.CommonProperty.ECR_REPOSITORY_PREFIX;
import static sleeper.core.properties.instance.CommonProperty.ID;

/**
 * TODO.
 */
public class DockerDeployment {

    public static final String INGEST_NAME = "ingest";
    public static final String EKS_BULK_IMPORT_NAME = "bulk-import-runner";
    public static final String COMPACTION_NAME = "compaction-job-execution";
    public static final String EMR_SERVERLESS_BULK_IMPORT_NAME = "bulk-import-runner-emr-serverless";
    public static final String BULK_EXPORT_NAME = "bulk-export-task-execution";

    private DockerDeployment() {
    }

    /**
     * Retrieves the name of an ECR repository.
     *
     * @param  instanceProperties the instance properties
     * @param  deploymentName     the Docker deployment to retrieve
     * @return                    the ECR repository name
     */
    public static String getEcrRepositoryName(InstanceProperties instanceProperties, String deploymentName) {
        return getEcrRepositoryPrefix(instanceProperties) + "/" + deploymentName;
    }

    /**
     * Retrieves the prefix of ECR repository names for a Sleeper instance.
     *
     * @param  properties the instance properties
     * @return            the ECR repository name
     */
    public static String getEcrRepositoryPrefix(InstanceProperties properties) {
        return Optional.ofNullable(properties.get(ECR_REPOSITORY_PREFIX)).orElseGet(() -> properties.get(ID));
    }
}
