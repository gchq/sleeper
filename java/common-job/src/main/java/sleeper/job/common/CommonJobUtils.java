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
package sleeper.job.common;

import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.model.ContainerInstance;
import com.amazonaws.services.ecs.model.DescribeContainerInstancesRequest;
import com.amazonaws.services.ecs.model.DescribeContainerInstancesResult;
import com.amazonaws.services.ecs.model.ListTasksRequest;
import com.amazonaws.util.EC2MetadataUtils;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Utility class common to Sleeper components that run jobs in a container and need to consume
 * messages from a queue.
 */
public class CommonJobUtils {
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

    private static final Logger LOGGER = LoggerFactory.getLogger(CommonJobUtils.class);

    private CommonJobUtils() {
    }

    /**
     * Return the instance information about this EC2 instance. This function doesn't work inside
     * containers on ECS.
     *
     * @return an optional containing the instance information
     */
    public static Optional<EC2MetadataUtils.InstanceInfo> getEC2Info() {
        if (ECS_EC2_ENV.equalsIgnoreCase(System.getenv(EXECUTION_ENV))) {
            return Optional.ofNullable(EC2MetadataUtils.getInstanceInfo());
        } else {
            return Optional.empty();
        }
    }

    /**
     * Basic metadata class about where this container is running.
     */
    public static class ContainerMetadata {
        public final String clusterName;
        public final String instanceARN;
        public final String instanceID;
        public final String az;
        public final String status;

        public ContainerMetadata(String clusterName, String instanceARN, String instanceID, String az, String status) {
            super();
            this.clusterName = clusterName;
            this.instanceARN = instanceARN;
            this.instanceID = instanceID;
            this.az = az;
            this.status = status;
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
            ContainerMetadata other = (ContainerMetadata) obj;
            return Objects.equals(az, other.az) && Objects.equals(clusterName, other.clusterName) && Objects.equals(instanceARN, other.instanceARN) && Objects.equals(instanceID, other.instanceID)
                    && Objects.equals(status, other.status);
        }

        @Override
        public String toString() {
            return "ContainerMetadata [clusterName=" + clusterName + ", instanceARN=" + instanceARN + ", instanceID=" + instanceID + ", az=" + az + ", status=" + status + "]";
        }
    }

    /**
     * Read the container metadata from Amazon ECS API and from the container metadata file.
     *
     * @param ecsClient the API client
     * @return container details if available
     * @throws IOException for an I/O error reading the metadata file
     */
    public static Optional<ContainerMetadata> retrieveContainerMetadata(AmazonECS ecsClient) throws IOException {
        if (ECS_EC2_ENV.equalsIgnoreCase(System.getenv(EXECUTION_ENV))) {
            Optional<Triple<String, String, String>> metadata = retrieveContainerMetadataViaEnvFile();
            if (metadata.isPresent()) {
                Optional<ContainerInstance> instanceDetails = describeContainerInstance(ecsClient, metadata.get().getLeft(), metadata.get().getMiddle());

                if (instanceDetails.isPresent()) {
                    return Optional.of(new ContainerMetadata(metadata.get().getLeft(), metadata.get().getMiddle(),
                            instanceDetails.get().getEc2InstanceId(), metadata.get().getRight(), instanceDetails.get().getStatus()));
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
     * @return a triple of cluster name, instanceARN and availability zone if the environment
     * variable is set
     * @throws IOException if error occurs reading file
     */
    public static Optional<Triple<String, String, String>> retrieveContainerMetadataViaEnvFile() throws IOException {
        String metaDataFile = System.getenv(ECS_CONTAINER_METADATA_FILE);
        if (metaDataFile != null) {
            return retrieveContainerMetadataFile(metaDataFile);
        } else {
            LOGGER.warn("Environment variable {} not set! Can't retrieve container metadata", ECS_CONTAINER_METADATA_FILE);
            return Optional.empty();
        }
    }

    /**
     * Get some container metadata from the container metadata file at the given path.
     *
     * @param file the path to the metadata
     * @return triple of cluster name, instanceARN and availability zone in that order
     * @throws IOException if an error occurs whilst retrieving data
     */
    public static Optional<Triple<String, String, String>> retrieveContainerMetadataFile(String file) throws IOException {
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
     * @param ecsClient            API client
     * @param ecsCluster           cluster name
     * @param containerInstanceARN EC2 instance ARN
     * @return container instance or an empty optional
     */
    public static Optional<ContainerInstance> describeContainerInstance(AmazonECS ecsClient, String ecsCluster, String containerInstanceARN) {
        Objects.requireNonNull(ecsClient);
        Objects.requireNonNull(ecsCluster, "ecsCluster");
        Objects.requireNonNull(containerInstanceARN, "containerInstanceARN");
        DescribeContainerInstancesRequest req = new DescribeContainerInstancesRequest()
                .withCluster(ecsCluster)
                .withContainerInstances(containerInstanceARN);
        DescribeContainerInstancesResult result = ecsClient.describeContainerInstances(req);
        if (result.getContainerInstances().isEmpty()) {
            LOGGER.warn("No EC2 instances returned in describeContainerInstance request!");
            return Optional.empty();
        } else {
            return Optional.of(result.getContainerInstances().get(0));
        }
    }

    public static int getNumPendingAndRunningTasks(String clusterName, AmazonECS ecsClient) throws ListRunningTasksException {
        ListTasksRequest listTasksRequest = new ListTasksRequest().withCluster(clusterName).withDesiredStatus("RUNNING");
        List<String> taskArns = ecsClient.listTasks(listTasksRequest).getTaskArns();
        if (null == taskArns || taskArns.size() != 1) {
            throw new ListRunningTasksException("Unable to retrieve details of cluster " + clusterName);
        }
        return taskArns.size();
    }
}
