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

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.core.properties.instance.CommonProperty.ECR_REPOSITORY_PREFIX;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.REGION;

/**
 * TODO.
 */
public class DockerDeployment {

    public static final DockerDeployment INGEST_NAME = new DockerDeployment("ingest");
    public static final DockerDeployment EKS_BULK_IMPORT_NAME = new DockerDeployment("bulk-import-runner");
    public static final DockerDeployment COMPACTION_NAME = new DockerDeployment("compaction-job-execution");
    public static final DockerDeployment EMR_SERVERLESS_BULK_IMPORT_NAME = new DockerDeployment("bulk-import-runner-emr-serverless");
    public static final DockerDeployment BULK_EXPORT_NAME = new DockerDeployment("bulk-export-task-execution");

    private final String deploymentName;

    private DockerDeployment(String deploymentName) {
        this.deploymentName = deploymentName;
    }

    /**
     * Retrieves the Docker image name, including tag.
     *
     * @param  properties       the instance properties
     * @param  dockerDeployment the Docker deployment to retrieve
     * @return                  the ECR repository name
     */
    public static String getDockerImageName(InstanceProperties properties, DockerDeployment dockerDeployment) {
        return properties.get(ACCOUNT) + ".dkr.ecr." +
                properties.get(REGION) + ".amazonaws.com/" +
                getEcrRepositoryName(properties, dockerDeployment) +
                ":" + properties.get(VERSION);
    }

    /**
     * Retrieves the Docker image name, including tag.
     *
     * @param  properties the instance properties
     * @return            the ECR repository name
     */
    public String getDockerImageName(InstanceProperties properties) {
        return properties.get(ACCOUNT) + ".dkr.ecr." +
                properties.get(REGION) + ".amazonaws.com/" +
                getEcrRepositoryName(properties) +
                ":" + properties.get(VERSION);
    }

    /**
     * Retrieves the name of an ECR repository.
     *
     * @param  instanceProperties the instance properties
     * @param  dockerDeployment   the Docker deployment to retrieve
     * @return                    the ECR repository name
     */
    public static String getEcrRepositoryName(InstanceProperties instanceProperties, DockerDeployment dockerDeployment) {
        return getEcrRepositoryPrefix(instanceProperties) + "/" + dockerDeployment.deploymentName;
    }

    /**
     * Retrieves the name of the ECR repository for this docker deployment.
     *
     * @param  instanceProperties the instance properties
     * @return                    the ECR repository name
     */
    public String getEcrRepositoryName(InstanceProperties instanceProperties) {
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
