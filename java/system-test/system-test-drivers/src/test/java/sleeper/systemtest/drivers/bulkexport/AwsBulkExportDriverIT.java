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
package sleeper.systemtest.drivers.bulkexport;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.bulkexport.core.model.BulkExportQuery;
import sleeper.bulkexport.core.model.BulkExportQuerySerDe;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.systemtest.drivers.testutil.LocalStackDslTest;
import sleeper.systemtest.drivers.testutil.LocalStackSystemTestDrivers;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.bulkexport.BulkExportDriver;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_EXPORT_QUEUE_URL;
import static sleeper.systemtest.drivers.testutil.LocalStackTestInstance.LOCALSTACK_MAIN;

@LocalStackDslTest
public class AwsBulkExportDriverIT extends LocalStackTestBase {

    public static final Logger LOGGER = LoggerFactory.getLogger(AwsBulkExportDriverIT.class);

    S3Client s3;
    SqsClient sqs;
    BulkExportDriver driver;
    InstanceProperties instanceProperties;

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, SystemTestContext context, LocalStackSystemTestDrivers drivers) {
        sleeper.connectToInstanceAddOfflineTable(LOCALSTACK_MAIN);
        s3 = drivers.clients().getS3();
        sqs = drivers.clients().getSqs();
        driver = drivers.bulkExport(context);
        instanceProperties = context.instance().getInstanceProperties();
    }

    @Test
    void shouldSendBulkExportJob() {
        // Given
        instanceProperties.set(BULK_EXPORT_QUEUE_URL, createSqsQueueGetUrl());
        BulkExportQuery query = BulkExportQuery.builder()
                .exportId("test-export")
                .tableName("table-name")
                .build();

        // When
        driver.sendJob(query);

        // Then
        assertThat(recieveBulkExportQueries()).containsExactly(query);
    }

    private List<BulkExportQuery> recieveBulkExportQueries() {
        return receiveMessages(instanceProperties.get(BULK_EXPORT_QUEUE_URL))
                .map(new BulkExportQuerySerDe()::fromJson)
                .toList();
    }

}
