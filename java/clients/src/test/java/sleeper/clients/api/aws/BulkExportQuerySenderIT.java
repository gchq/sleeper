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
package sleeper.clients.api.aws;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.Message;

import sleeper.bulkexport.core.model.BulkExportQuery;
import sleeper.bulkexport.core.model.BulkExportQuerySerDe;
import sleeper.clients.api.BulkExportQuerySender;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.localstack.test.LocalStackTestBase;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_EXPORT_QUEUE_URL;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

public class BulkExportQuerySenderIT extends LocalStackTestBase {

    InstanceProperties instanceProperties = createTestInstanceProperties();

    @BeforeEach
    void setUp() {
        instanceProperties.set(BULK_EXPORT_QUEUE_URL, createSqsQueueGetUrl());
    }

    @Test
    void shouldExportTableUsingBulkExportQuery() {
        String exportId = UUID.randomUUID().toString();
        BulkExportQuery query = BulkExportQuery.builder()
                .tableName("export-table")
                .exportId(exportId)
                .build();

        BulkExportQuerySender.toSqs(instanceProperties, sqsClient)
                .sendQueryToBulkExport(query);
        assertThat(recieveBulkExportQueries()).containsExactly(query);
    }

    private List<BulkExportQuery> recieveBulkExportQueries() {
        return sqsClient.receiveMessage(request -> request
                .queueUrl(instanceProperties.get(BULK_EXPORT_QUEUE_URL))
                .maxNumberOfMessages(10)
                .waitTimeSeconds(1))
                .messages().stream()
                .map(Message::body)
                .map(new BulkExportQuerySerDe()::fromJson)
                .toList();
    }
}
