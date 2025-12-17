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

package sleeper.clients.teardown;

import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.cloudwatchevents.CloudWatchEventsClient;

import java.io.IOException;
import java.util.Objects;

/**
 * Create clients used in teardown.
 */
public class TearDownClients {

    private final CloudWatchEventsClient cloudWatch;
    private final CloudFormationClient cloudFormation;

    private TearDownClients(Builder builder) {
        cloudWatch = Objects.requireNonNull(builder.cloudWatch, "cloudWatch must not be null");
        cloudFormation = Objects.requireNonNull(builder.cloudFormation, "cloudFormation must not be null");
    }

    /**
     * Create default clients to use in teardown.
     *
     * @param  operation            the tear down operation
     * @throws IOException          if an IO error occurs
     * @throws InterruptedException the thread was interrupted while waiting
     */
    public static void withDefaults(TearDownOperation operation) throws IOException, InterruptedException {
        try (CloudWatchEventsClient cloudWatchClient = CloudWatchEventsClient.create();
                CloudFormationClient cloudFormationClient = CloudFormationClient.create()) {
            TearDownClients clients = builder()
                    .cloudWatch(cloudWatchClient)
                    .cloudFormation(cloudFormationClient)
                    .build();
            operation.tearDown(clients);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public CloudWatchEventsClient getCloudWatch() {
        return cloudWatch;
    }

    public CloudFormationClient getCloudFormation() {
        return cloudFormation;
    }

    /**
     * A builder for instances of this class.
     */
    public static final class Builder {
        private CloudWatchEventsClient cloudWatch;
        private CloudFormationClient cloudFormation;

        private Builder() {
        }

        /**
         * Set the cloud watch client.
         *
         * @param  cloudWatch the cloud watch client
         * @return            this builder
         */
        public Builder cloudWatch(CloudWatchEventsClient cloudWatch) {
            this.cloudWatch = cloudWatch;
            return this;
        }

        /**
         * Set the cloud formation client.
         *
         * @param  cloudFormation the cloud formation client
         * @return                this builder
         */
        public Builder cloudFormation(CloudFormationClient cloudFormation) {
            this.cloudFormation = cloudFormation;
            return this;
        }

        public TearDownClients build() {
            return new TearDownClients(this);
        }
    }

    /**
     * An interface to the teardown operation.
     */
    public interface TearDownOperation {

        /**
         * Teardown a Sleeper stack.
         *
         * @param  clients              the clients
         * @throws IOException          if an IO error occurs
         * @throws InterruptedException the thread was interrupted while waiting
         */
        void tearDown(TearDownClients clients) throws IOException, InterruptedException;
    }
}
