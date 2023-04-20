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
package sleeper.clients.status.report.partitions;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.clients.testutil.ToStringPrintStream;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.status.report.partitions.PartitionStatusReportTestHelper.createRootPartitionWithTwoChildren;
import static sleeper.clients.testutil.ClientTestUtils.example;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.statestore.dynamodb.DynamoDBStateStoreTestHelper.createTestTable;

@Testcontainers
public class PartitionsStatusReportIT {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(
            DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);

    private final AmazonS3 s3 = createS3Client();
    private final AmazonDynamoDB dynamoDB = createDynamoClient();
    private final InstanceProperties instanceProperties = createTestInstanceProperties(s3);
    private final Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
    private final TableProperties tableProperties = createTestTable(
            instanceProperties, schema, s3, dynamoDB,
            tableProperties -> tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 10));

    @Test
    void shouldGetReportWhenTwoLeafPartitionsBothNeedSplitting() throws Exception {
        // Given
        createRootPartitionWithTwoChildren()
                .singleFileInEachLeafPartitionWithRecords(100)
                .setupStateStore(stateStore());

        // When
        ToStringPrintStream out = new ToStringPrintStream();
        PartitionsStatusReportArguments.fromArgs(
                        instanceProperties.get(ID), tableProperties.get(TABLE_NAME))
                .runReport(s3, dynamoDB, out.getPrintStream());

        // Then
        assertThat(out).hasToString(
                example("reports/partitions/rootWithTwoChildrenBothNeedSplitting.txt"));
    }

    private static AmazonS3 createS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3))
                .build();
    }

    private static AmazonDynamoDB createDynamoClient() {
        return AmazonDynamoDBClientBuilder.standard()
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.DYNAMODB))
                .build();
    }

    private StateStore stateStore() {
        return new StateStoreProvider(dynamoDB, instanceProperties, new Configuration()).getStateStore(tableProperties);
    }
}
