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

/**
 * AWS clients used when tearing down Sleeper instances and system test deployments.
 */
public class TearDownClients {

    private final CloudWatchEventsClient cloudWatch;
    private final CloudFormationClient cloudFormation;

    private TearDownClients(CloudWatchEventsClient cloudWatch, CloudFormationClient cloudFormation) {
        this.cloudWatch = cloudWatch;
        this.cloudFormation = cloudFormation;
    }

    /**
     * Performs some operation that needs an instance of this class. Creates AWS clients and then closes them at the
     * end. This is a convenience for use in a main method.
     *
     * @param  operation            the operation to perform with the clients
     * @throws IOException          if the operation threw an IOException
     * @throws InterruptedException if the operation threw an InterruptedException
     */
    public static void withDefaults(TearDownOperation operation) throws IOException, InterruptedException {
        try (CloudWatchEventsClient cloudWatchClient = CloudWatchEventsClient.create();
                CloudFormationClient cloudFormationClient = CloudFormationClient.create()) {
            TearDownClients clients = new TearDownClients(cloudWatchClient, cloudFormationClient);
            operation.tearDown(clients);
        }
    }

    public CloudWatchEventsClient getCloudWatch() {
        return cloudWatch;
    }

    public CloudFormationClient getCloudFormation() {
        return cloudFormation;
    }

    /**
     * An operation that deletes one or more Sleeper instances and/or system test deployments. The tear down clients
     * will be passed as a parameter.
     */
    public interface TearDownOperation {

        /**
         * Performs the operation.
         *
         * @param  clients              the tear down clients
         * @throws IOException          if an IOException occurs, usually when removing a local copy of configuration
         *                              files
         * @throws InterruptedException if an InterruptedException occurs, usually while waiting for tear down to finish
         */
        void tearDown(TearDownClients clients) throws IOException, InterruptedException;
    }
}
