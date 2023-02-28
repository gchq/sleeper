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

package sleeper.clients.admin.deploy;

import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.model.ListTasksRequest;
import com.amazonaws.services.ecs.model.StopTaskRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.InstanceProperties;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_CLUSTER;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_TASK_CREATION_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_CLUSTER;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_LAMBDA_FUNCTION;

public class RestartTasks {

    private static final Logger LOGGER = LoggerFactory.getLogger(RestartTasks.class);
    private final AmazonECS ecs;
    private final InstanceProperties properties;

    private RestartTasks(Builder builder) {
        ecs = builder.ecs;
        properties = builder.properties;
    }

    public static Builder builder() {
        return new Builder();
    }

    public void run() {
        stopTasks();
        startTasks();
    }

    public void stopTasks() {
        LOGGER.info("Stopping tasks in ingest cluster");
        stopTasksInCluster(properties.get(INGEST_CLUSTER));
        LOGGER.info("Stopping tasks in compaction cluster");
        stopTasksInCluster(properties.get(COMPACTION_CLUSTER));
    }

    public void stopTasksInCluster(String cluster) {
        ecs.listTasks(new ListTasksRequest().withCluster(cluster)).getTaskArns().forEach(task ->
                ecs.stopTask(new StopTaskRequest().withTask(task).withCluster(cluster))
        );
    }

    public void startTasks() {
        LOGGER.info("Invoking ingest task lambda");
        InvokeLambda.invoke(properties.get(INGEST_LAMBDA_FUNCTION));
        LOGGER.info("Invoking compaction task lambda");
        InvokeLambda.invoke(properties.get(COMPACTION_TASK_CREATION_LAMBDA_FUNCTION));
    }

    public static final class Builder {
        private AmazonECS ecs;
        private InstanceProperties properties;

        private Builder() {
        }

        public Builder ecs(AmazonECS ecs) {
            this.ecs = ecs;
            return this;
        }

        public Builder properties(InstanceProperties properties) {
            this.properties = properties;
            return this;
        }

        public RestartTasks build() {
            return new RestartTasks(this);
        }
    }
}
