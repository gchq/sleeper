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
package sleeper.bulkimport.runner;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.bulkimport.starter.executor.BulkImportJobWriterToS3;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.localstack.test.LocalStackTestBase;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

public class BulkImportJobLoaderFromS3IT extends LocalStackTestBase {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();

    @BeforeEach
    void setUp() {
        instanceProperties.set(BULK_IMPORT_BUCKET, UUID.randomUUID().toString());
        createBucket(instanceProperties.get(BULK_IMPORT_BUCKET));
    }

    @Test
    void shouldLoadBulkImportJobFromS3() {
        // Given
        String jobRunId = "load-run";
        String jobId = "load-job-id";

        BulkImportJob bulkImportJob = BulkImportJob.builder()
                .id(jobId)
                .tableId("test-table-id")
                .files(List.of("/load-job.parquet"))
                .build();

        BulkImportJobWriterToS3 bulkImportJobWriterToS3 = new BulkImportJobWriterToS3(instanceProperties, s3Client);
        bulkImportJobWriterToS3.writeJobToBulkImportBucket(bulkImportJob, jobRunId);

        // When / Then
        assertThat(BulkImportJobLoaderFromS3.loadJob(instanceProperties, jobId, jobRunId, s3Client))
                .isEqualTo(bulkImportJob);
        // And the file is deleted after it is loaded
        assertThat(listObjectKeys(instanceProperties.get(BULK_IMPORT_BUCKET))).isEmpty();
    }
}
