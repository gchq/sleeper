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

package sleeper.clients.status.report.query;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.CommonTestConstants;
import sleeper.query.tracker.DynamoDBQueryTracker;
import sleeper.query.tracker.TrackedQuery;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.status.report.query.QueryTrackerReporterTestHelper.getStandardReport;
import static sleeper.clients.testutil.ClientTestUtils.example;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;

@Testcontainers
public class StandardQueryTrackerReporterIT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.DYNAMODB);

    private final AmazonDynamoDB dynamoDB = buildAwsV1Client(
            localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    private final InstanceProperties instanceProperties = new InstanceProperties();
    private final DynamoDBQueryTracker dynamoDBQueryTracker = new DynamoDBQueryTracker(instanceProperties, dynamoDB);

    @Nested
    @DisplayName("All tracked queries")
    class AllTrackedQueries {
        @Test
        void shouldRunReportWithNoTrackedQueries() throws Exception {
            List<TrackedQuery> noQueries = List.of();

            assertThat(getStandardReport(TrackerQuery.ALL, noQueries))
                    .isEqualTo(example("reports/query/noQueries.txt"));
        }
    }
}
