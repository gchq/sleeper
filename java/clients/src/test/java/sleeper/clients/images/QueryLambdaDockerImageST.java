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
package sleeper.clients.images;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.deploy.LambdaHandler;
import sleeper.query.runner.tracker.DynamoDBQueryTrackerCreator;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class QueryLambdaDockerImageST extends LambdaDockerImageTestBase {

    @BeforeEach
    void setUp() {
        createBucket(instanceProperties.get(QUERY_RESULTS_BUCKET));
        new DynamoDBQueryTrackerCreator(instanceProperties, dynamoClient).create();
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);
        update(stateStore).initialise(tableProperties);
    }

    @Test
    void shouldRunASubQuery() throws Exception {
        runLambda("query-lambda:test", LambdaHandler.QUERY_LEAF_PARTITION, "{\"Records\":[]}");
    }

}
