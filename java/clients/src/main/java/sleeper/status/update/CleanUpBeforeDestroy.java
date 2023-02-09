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
package sleeper.status.update;

import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.AmazonECSClientBuilder;
import com.amazonaws.services.ecs.model.ListTasksRequest;
import com.amazonaws.services.ecs.model.ListTasksResult;
import com.amazonaws.services.ecs.model.StopTaskRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.iterable.S3Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.local.LoadLocalProperties;
import sleeper.configuration.properties.table.TableProperties;

import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_CLUSTER;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_CLUSTER;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_CLUSTER;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.RETAIN_INFRA_AFTER_DESTROY;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.util.RateLimitUtils.sleepForSustainedRatePerSecond;

public class CleanUpBeforeDestroy {
    private static final Logger LOGGER = LoggerFactory.getLogger(CleanUpBeforeDestroy.class);

    private CleanUpBeforeDestroy() {
    }

    public static void main(String[] args) {
        if (1 != args.length) {
            throw new IllegalArgumentException("Usage: <configuration directory>");
        }
        Path baseDir = Path.of(args[0]);
        InstanceProperties instanceProperties = LoadLocalProperties.loadInstancePropertiesFromDirectory(baseDir);
        List<TableProperties> tablePropertiesList = LoadLocalProperties
                .loadTablesFromDirectory(instanceProperties, baseDir).collect(Collectors.toList());

        cleanUp(instanceProperties, tablePropertiesList, List.of());
    }

    public static void cleanUp(
            InstanceProperties instanceProperties,
            List<TableProperties> tableProperties,
            List<String> extraECSClusters) {
        LOGGER.info("Pausing the system");
        PauseSystem.pause(instanceProperties);
        stopECSTasks(instanceProperties, extraECSClusters);
        cleanUnretainedInfra(instanceProperties, tableProperties);
    }

    private static void cleanUnretainedInfra(
            InstanceProperties instanceProperties, List<TableProperties> tablePropertiesList) {
        if (!instanceProperties.getBoolean(RETAIN_INFRA_AFTER_DESTROY)) {
            LOGGER.info("Removing all data from config, table and query results buckets");
            AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
            emptyBucket(s3, instanceProperties.get(CONFIG_BUCKET));
            emptyBucket(s3, instanceProperties.get(QUERY_RESULTS_BUCKET));
            tablePropertiesList.forEach(tableProperties -> emptyBucket(s3, tableProperties.get(DATA_BUCKET)));
            s3.shutdown();
        }
    }

    private static void stopECSTasks(InstanceProperties instanceProperties, List<String> extraClusters) {
        AmazonECS ecs = AmazonECSClientBuilder.defaultClient();
        stopTasks(ecs, instanceProperties.get(INGEST_CLUSTER));
        stopTasks(ecs, instanceProperties.get(COMPACTION_CLUSTER));
        stopTasks(ecs, instanceProperties.get(SPLITTING_COMPACTION_CLUSTER));
        extraClusters.forEach(clusterName -> stopTasks(ecs, clusterName));
        ecs.shutdown();
    }

    private static void emptyBucket(AmazonS3 s3, String bucketName) {
        try {
            S3Objects.inBucket(s3, bucketName).forEach(object ->
                    s3.deleteObject(bucketName, object.getKey()));
        } catch (Exception e) {
            LOGGER.warn("Failed emptying S3 bucket {}, continuing", bucketName, e);
        }
    }

    private static void stopTasks(AmazonECS ecs, String clusterName) {
        LOGGER.info("Stopping tasks for ECS cluster {}", clusterName);
        forEachTaskArn(ecs, clusterName, taskArn -> {
            // Rate limit for ECS StopTask is 100 burst, 40 sustained:
            // https://docs.aws.amazon.com/AmazonECS/latest/APIReference/request-throttling.html
            sleepForSustainedRatePerSecond(30);
            ecs.stopTask(new StopTaskRequest()
                    .withCluster(clusterName)
                    .withTask(taskArn)
                    .withReason("Cleaning up before cdk destroy"));
        });
    }

    private static void forEachTaskArn(AmazonECS ecs, String clusterName, Consumer<String> consumer) {
        String nextToken = null;
        do {
            ListTasksResult result = ecs.listTasks(new ListTasksRequest()
                    .withCluster(clusterName).withNextToken(nextToken));

            LOGGER.info("Found {} tasks", result.getTaskArns().size());
            result.getTaskArns().forEach(consumer);
            nextToken = result.getNextToken();
        } while (nextToken != null);
    }
}
