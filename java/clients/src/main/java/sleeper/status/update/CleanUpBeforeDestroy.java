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
import static sleeper.configuration.properties.UserDefinedInstanceProperty.RETAIN_INFRA_AFTER_DESTROY;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;

public class CleanUpBeforeDestroy {

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

        PauseSystem.pause(instanceProperties);
        stopECSTasks(instanceProperties);
        cleanUnretainedInfra(instanceProperties, tablePropertiesList);
    }

    private static void cleanUnretainedInfra(
            InstanceProperties instanceProperties, List<TableProperties> tablePropertiesList) {
        if (!instanceProperties.getBoolean(RETAIN_INFRA_AFTER_DESTROY)) {
            System.out.println("Removing all data from config, table and query results buckets");
            AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
            emptyBucket(s3, instanceProperties.get(CONFIG_BUCKET));
            emptyBucket(s3, instanceProperties.get(QUERY_RESULTS_BUCKET));
            tablePropertiesList.forEach(tableProperties -> emptyBucket(s3, tableProperties.get(DATA_BUCKET)));
            s3.shutdown();
        }
    }

    private static void stopECSTasks(InstanceProperties instanceProperties) {
        AmazonECS ecs = AmazonECSClientBuilder.defaultClient();
        stopTasks(ecs, instanceProperties.get(INGEST_CLUSTER));
        stopTasks(ecs, instanceProperties.get(COMPACTION_CLUSTER));
    }

    private static void emptyBucket(AmazonS3 s3, String bucketName) {
        S3Objects.inBucket(s3, bucketName).forEach(object ->
                s3.deleteObject(bucketName, object.getKey()));
    }

    private static void stopTasks(AmazonECS ecs, String clusterName) {
        forEachTaskArn(ecs, clusterName, taskArn -> {
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
            result.getTaskArns().forEach(consumer);
            nextToken = result.getNextToken();
        } while (nextToken != null);
    }
}
