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

package sleeper.clients.deploy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ecs.EcsClient;
import software.amazon.awssdk.services.lambda.LambdaClient;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.instance.InstanceProperty;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_CLUSTER;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_TASK_CREATION_LAMBDA_FUNCTION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_CLUSTER;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_LAMBDA_FUNCTION;

public class RestartTasks {

    private static final Logger LOGGER = LoggerFactory.getLogger(RestartTasks.class);
    private final EcsClient ecs;
    private final LambdaClient lambda;
    private final InstanceProperties properties;
    private final boolean skip;

    private RestartTasks(Builder builder) {
        ecs = builder.ecs;
        lambda = builder.lambda;
        properties = builder.properties;
        skip = builder.skip;
    }

    public static Builder builder() {
        return new Builder();
    }

    public void run() {
        if (skip) {
            LOGGER.info("Not restarting ECS tasks");
            return;
        }
        restartTasks(INGEST_CLUSTER, INGEST_LAMBDA_FUNCTION);
        restartTasks(COMPACTION_CLUSTER, COMPACTION_TASK_CREATION_LAMBDA_FUNCTION);
    }

    private void restartTasks(InstanceProperty clusterProperty, InstanceProperty lambdaFunctionProperty) {
        String clusterName = properties.get(clusterProperty);
        if (clusterName != null) {
            LOGGER.info("Stopping tasks in cluster {}", clusterName);
            stopTasksInCluster(clusterName);
            LOGGER.info("Invoking lambda {}", properties.get(lambdaFunctionProperty));
            InvokeLambda.invokeWith(lambda, properties.get(lambdaFunctionProperty));
        }
    }

    private void stopTasksInCluster(String cluster) {
        ecs.listTasks(builder -> builder.cluster(cluster)).taskArns()
                .forEach(task -> ecs.stopTask(builder -> builder.cluster(cluster).task(task)));
    }

    public static final class Builder {
        private EcsClient ecs;
        private LambdaClient lambda;
        private InstanceProperties properties;
        private boolean skip;

        private Builder() {
        }

        public Builder ecs(EcsClient ecs) {
            this.ecs = ecs;
            return this;
        }

        public Builder lambda(LambdaClient lambda) {
            this.lambda = lambda;
            return this;
        }

        public Builder properties(InstanceProperties properties) {
            this.properties = properties;
            return this;
        }

        public Builder skipIf(boolean skip) {
            this.skip = skip;
            return this;
        }

        public RestartTasks build() {
            return new RestartTasks(this);
        }
    }
}
