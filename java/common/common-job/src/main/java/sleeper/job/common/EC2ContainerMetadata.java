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
package sleeper.job.common;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ecs.EcsClient;
import software.amazon.awssdk.services.ecs.model.ContainerInstance;
import software.amazon.awssdk.services.ecs.model.DescribeContainerInstancesResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;

/**
 * Retrieves metadata about where a container is running in ECS, if the container was deployed with EC2 rather than
 * Fargate.
 */
public class EC2ContainerMetadata {
    public static final Logger LOGGER = LoggerFactory.getLogger(EC2ContainerMetadata.class);

    /**
     * Environment variable key for finding where this program is running.
     */
    public static final String EXECUTION_ENV = "AWS_EXECUTION_ENV";

    /**
     * Value if running on EC2.
     */
    public static final String ECS_EC2_ENV = "AWS_ECS_EC2";

    /**
     * Environment variable for location of container metadata.
     */
    public static final String ECS_CONTAINER_METADATA_FILE = "ECS_CONTAINER_METADATA_FILE";

    public final String clusterName;
    public final String instanceARN;
    public final String instanceID;
    public final String az;
    public final String status;

    public EC2ContainerMetadata(String clusterName, String instanceARN, String instanceID, String az, String status) {
        super();
        this.clusterName = clusterName;
        this.instanceARN = instanceARN;
        this.instanceID = instanceID;
        this.az = az;
        this.status = status;
    }

    /**
     * Read the container metadata from Amazon ECS API and from the container metadata file.
     *
     * @param  ecsClient   the API client
     * @return             container details if available
     * @throws IOException for an I/O error reading the metadata file
     */
    public static Optional<EC2ContainerMetadata> retrieveContainerMetadata(EcsClient ecsClient) throws IOException {
        if (ECS_EC2_ENV.equalsIgnoreCase(System.getenv(EXECUTION_ENV))) {
            Optional<Triple<String, String, String>> metadata = retrieveContainerMetadataViaEnvFile();
            if (metadata.isPresent()) {
                Optional<ContainerInstance> instanceDetails = describeContainerInstance(ecsClient, metadata.get().getLeft(), metadata.get().getMiddle());

                if (instanceDetails.isPresent()) {
                    return Optional.of(new EC2ContainerMetadata(metadata.get().getLeft(), metadata.get().getMiddle(),
                            instanceDetails.get().ec2InstanceId(), metadata.get().getRight(), instanceDetails.get().status()));
                }
            }
        } else {
            LOGGER.info("Not running on an EC2 instance");
        }
        return Optional.empty();
    }

    /**
     * Get some container metadata from the metadata file stored at the place indicated by AWS
     * environment variable.
     *
     * @return             a triple of cluster name, instanceARN and availability zone if the environment
     *                     variable is set
     * @throws IOException if error occurs reading file
     */
    private static Optional<Triple<String, String, String>> retrieveContainerMetadataViaEnvFile() throws IOException {
        String metaDataFile = System.getenv(EC2ContainerMetadata.ECS_CONTAINER_METADATA_FILE);
        if (metaDataFile != null) {
            return retrieveContainerMetadataFile(metaDataFile);
        } else {
            LOGGER.warn("Environment variable {} not set! Can't retrieve container metadata", EC2ContainerMetadata.ECS_CONTAINER_METADATA_FILE);
            return Optional.empty();
        }
    }

    /**
     * Get some container metadata from the container metadata file at the given path.
     *
     * @param  file        the path to the metadata
     * @return             triple of cluster name, instanceARN and availability zone in that order
     * @throws IOException if an error occurs whilst retrieving data
     */
    private static Optional<Triple<String, String, String>> retrieveContainerMetadataFile(String file) throws IOException {
        Objects.requireNonNull(file, "file");

        BufferedReader metaReader = Files.newBufferedReader(Paths.get(file));
        JsonReader jsread = new JsonReader(metaReader);

        JsonElement root = JsonParser.parseReader(jsread);
        JsonObject rootNode = root.getAsJsonObject();
        String clusterName = rootNode.get("Cluster").getAsString();
        String containerInstanceArn = rootNode.get("ContainerInstanceARN").getAsString();
        String az = rootNode.get("AvailabilityZone").getAsString();
        return Optional.of(Triple.of(clusterName, containerInstanceArn, az));
    }

    /**
     * Get information about the EC2 instance named by ARN.
     *
     * @param  ecsClient            API client
     * @param  ecsCluster           cluster name
     * @param  containerInstanceARN EC2 instance ARN
     * @return                      container instance or an empty optional
     */
    private static Optional<ContainerInstance> describeContainerInstance(EcsClient ecsClient, String ecsCluster, String containerInstanceARN) {
        Objects.requireNonNull(ecsClient);
        Objects.requireNonNull(ecsCluster, "ecsCluster");
        Objects.requireNonNull(containerInstanceARN, "containerInstanceARN");
        DescribeContainerInstancesResponse response = ecsClient.describeContainerInstances(request -> request
                .cluster(ecsCluster).containerInstances(containerInstanceARN));
        if (response.containerInstances().isEmpty()) {
            LOGGER.warn("No EC2 instances returned in describeContainerInstance request!");
            return Optional.empty();
        } else {
            return Optional.of(response.containerInstances().get(0));
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(az, clusterName, instanceARN, instanceID, status);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        EC2ContainerMetadata other = (EC2ContainerMetadata) obj;
        return Objects.equals(az, other.az) && Objects.equals(clusterName, other.clusterName) && Objects.equals(instanceARN, other.instanceARN) && Objects.equals(instanceID, other.instanceID)
                && Objects.equals(status, other.status);
    }

    @Override
    public String toString() {
        return "ContainerMetadata [clusterName=" + clusterName + ", instanceARN=" + instanceARN + ", instanceID=" + instanceID + ", az=" + az + ", status=" + status + "]";
    }
}
