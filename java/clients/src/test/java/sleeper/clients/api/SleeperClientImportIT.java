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
package sleeper.clients.api;

import com.amazonaws.services.sqs.model.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.bulkimport.core.job.BulkImportJobSerDe;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.localstack.test.LocalStackTestBase;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_JOB_QUEUE_URL;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

public class SleeperClientImportIT extends LocalStackTestBase {

    InstanceProperties instanceProperties = createTestInstanceProperties();

    @BeforeEach
    void setUp() {
        instanceProperties.set(BULK_IMPORT_EMR_JOB_QUEUE_URL, createSqsQueueGetUrl());
    }

    @Test
    void shouldImportParquetFilesFromS3() {
        String jobId = UUID.randomUUID().toString();
        BulkImportJob job = BulkImportJob.builder()
                .tableName("Import-table")
                .id(jobId)
                .files(List.of("filename1.parquet", "filename2.parquet"))
                .build();

        SleeperClientImport.bulkImportParquetFilesFromS3(instanceProperties.get(BULK_IMPORT_EMR_JOB_QUEUE_URL), sqsClient)
                .bulkImportFilesFromS3(job);
        assertThat(recieveImportJobs()).containsExactly(job);
    }

    private List<BulkImportJob> recieveImportJobs() {
        return sqsClient.receiveMessage(instanceProperties.get(BULK_IMPORT_EMR_JOB_QUEUE_URL))
                .getMessages().stream()
                .map(Message::getBody)
                .map(new BulkImportJobSerDe()::fromJson)
                .toList();
    }
}
