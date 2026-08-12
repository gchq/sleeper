/*
 * Copyright 2022-2026 Crown Copyright
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
        String objectKey = "folder/test.json";

        BulkImportJob bulkImportJob = BulkImportJob.builder()
                .id("load-job-id")
                .tableId("test-table-id")
                .files(List.of("/load-job.parquet"))
                .build();

        // When
        writer().writeJobToBulkImportBucket(bulkImportJob, objectKey);
        BulkImportJob foundJob = loadJob(objectKey);

        // Then
        assertThat(foundJob).isEqualTo(bulkImportJob);
        // And the file is kept
        assertThat(listObjectKeys(instanceProperties.get(BULK_IMPORT_BUCKET))).containsExactly(objectKey);
    }

    private BulkImportJobWriterToS3 writer() {
        return new BulkImportJobWriterToS3(instanceProperties, s3Client);
    }

    private BulkImportJob loadJob(String objectKey) {
        return BulkImportJobLoaderFromS3.loadJob(instanceProperties, objectKey, s3Client);
    }
}
