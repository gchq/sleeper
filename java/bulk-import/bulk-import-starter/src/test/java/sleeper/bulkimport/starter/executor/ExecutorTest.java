/*
 * Copyright 2022 Crown Copyright
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
package sleeper.bulkimport.starter.executor;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.collect.Lists;
import java.util.Map;
import java.util.UUID;
import static org.junit.Assert.*;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import sleeper.bulkimport.job.BulkImportJob;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;
import sleeper.core.CommonTestConstants;
import sleeper.core.schema.Schema;

public class ExecutorTest {

    @ClassRule
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3);

    private AmazonS3 createS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3))
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .build();
    }

    @Test
    public void shouldCallRunOnPlatformIfJobIsValid() {
        // Given
        AmazonS3 s3 = createS3Client();
        String bucketName = UUID.randomUUID().toString();
        s3.createBucket(bucketName);
        s3.putObject(bucketName, "file1.parquet", "");
        s3.putObject(bucketName, "file2.parquet", "");
        s3.putObject(bucketName, "directory/file3.parquet", "");
        BulkImportJob importJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .files(Lists.newArrayList(bucketName + "/file1.parquet", bucketName + "/file2.parquet", bucketName + "/directory/file3.parquet"))
                .build();
        TablePropertiesProvider tablePropertiesProvider = new TestTablePropertiesProvider(new Schema());
        ExecutorMock executorMock = new ExecutorMock(new InstanceProperties(), tablePropertiesProvider, s3);

        // When
        executorMock.runJob(importJob);

        // Then
        assertTrue(executorMock.isRunJobOnPlatformCalled());
        s3.shutdown();
    }

    @Test
    public void shouldFailIfFileListIsEmpty() {
        // Given
        AmazonS3 s3 = createS3Client();
        String bucketName = UUID.randomUUID().toString();
        s3.createBucket(bucketName);
        BulkImportJob importJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .files(Lists.newArrayList())
                .build();
        TablePropertiesProvider tablePropertiesProvider = new TestTablePropertiesProvider(new Schema());
        ExecutorMock executorMock = new ExecutorMock(new InstanceProperties(), tablePropertiesProvider, s3);

        // When
        Exception exception = assertThrows(IllegalArgumentException.class, () -> executorMock.runJob(importJob));
        assertEquals(getExpectedErrorMessage("The input files must be set to a non-null and non-empty value."), exception.getMessage());
        s3.shutdown();
    }

    @Test
    public void shouldSucceedIfS3ObjectIsADirectoryContainingFiles() {
        // Given
        AmazonS3 s3 = createS3Client();
        String bucketName = UUID.randomUUID().toString();
        s3.createBucket(bucketName);
        s3.putObject(bucketName, "directory/file1.parquet", "");
        BulkImportJob importJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .files(Lists.newArrayList(bucketName + "/directory", bucketName + "/directory/"))
                .build();
        TablePropertiesProvider tablePropertiesProvider = new TestTablePropertiesProvider(new Schema());
        ExecutorMock executorMock = new ExecutorMock(new InstanceProperties(), tablePropertiesProvider, s3);

        // When
        executorMock.runJob(importJob);
        
        // Then
        assertTrue(executorMock.isRunJobOnPlatformCalled());
        s3.shutdown();
    }

    @Test
    public void shouldFailIfJobPointsAtNonExistentTable() {
        // Given
        AmazonS3 s3 = createS3Client();
        BulkImportJob importJob = new BulkImportJob.Builder()
                .tableName("table-that-does-not-exist")
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();
        TablePropertiesProvider tablePropertiesProvider = new TestTablePropertiesProvider(new Schema());
        ExecutorMock executorMock = new ExecutorMock(new InstanceProperties(), tablePropertiesProvider, s3);
        
        // When / Then
        Exception exception = assertThrows(IllegalArgumentException.class, () -> executorMock.runJob(importJob));
        assertEquals(getExpectedErrorMessage("Table does not exist."), exception.getMessage());
        s3.shutdown();
    }

    @Test
    public void shouldThrowExceptionIfJobIdContainsMoreThan63Characters() {
        // Given
        String invalidId = UUID.randomUUID().toString() + UUID.randomUUID();
        BulkImportJob importJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .files(Lists.newArrayList("file1.parquet"))
                .id(invalidId)
                .build();
        TablePropertiesProvider tablePropertiesProvider = new TestTablePropertiesProvider(new Schema());
        ExecutorMock executorMock = new ExecutorMock(new InstanceProperties(), tablePropertiesProvider, null);

        // When / Then
        Exception exception = assertThrows(IllegalArgumentException.class, () -> executorMock.runJob(importJob));
        assertEquals(getExpectedErrorMessage("Job IDs are only allowed to be up to 63 characters long."), exception.getMessage());
    }

    @Test
    public void shouldThrowExceptionIfTableNameIsNull() {
        // Given
        BulkImportJob importJob = new BulkImportJob.Builder()
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();
        TablePropertiesProvider tablePropertiesProvider = new TestTablePropertiesProvider(new Schema());
        ExecutorMock executorMock = new ExecutorMock(new InstanceProperties(), tablePropertiesProvider, null);

        // When / Then
        Exception exception = assertThrows(IllegalArgumentException.class, () -> executorMock.runJob(importJob));
        assertEquals(getExpectedErrorMessage("The table name must be set to a non-null value."), exception.getMessage());
    }

    @Test
    public void shouldThrowExceptionIfJobIdContainsUppercaseLetters() {
        // Given
        BulkImportJob importJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("importJob")
                .files(Lists.newArrayList("file1.parquet"))
                .build();
        TablePropertiesProvider tablePropertiesProvider = new TestTablePropertiesProvider(new Schema());
        ExecutorMock executorMock = new ExecutorMock(new InstanceProperties(), tablePropertiesProvider, null);

        // When / Then
        Exception exception = assertThrows(IllegalArgumentException.class, () -> executorMock.runJob(importJob));
        assertEquals(getExpectedErrorMessage("Job Ids must only contain lowercase alphanumerics and dashes."), exception.getMessage());
    }

    @Test
    public void shouldDoNothingWhenJobIsNull() {
        // Given
        ExecutorMock executorMock = new ExecutorMock(new InstanceProperties(), null, null);

        // When
        executorMock.runJob(null);

        // Then
        assertFalse(executorMock.isRunJobOnPlatformCalled());
    }

    private String getExpectedErrorMessage(String message) {
        return "The bulk import job failed validation with the following checks failing: \n"
                + message;
    }
     
    private static class ExecutorMock extends Executor {
        private boolean runJobOnPlatformCalled = false;

        public boolean isRunJobOnPlatformCalled() {
            return runJobOnPlatformCalled;
        }

        public ExecutorMock(InstanceProperties instanceProperties,
                            TablePropertiesProvider tablePropertiesProvider,
                            AmazonS3 s3) {
            super(instanceProperties, tablePropertiesProvider, s3);
        }

        @Override
        protected void runJobOnPlatform(BulkImportJob bulkImportJob) {
            runJobOnPlatformCalled = true;
        }

        @Override
        protected Map<String, String> getDefaultSparkConfig(BulkImportJob bulkImportJob, Map<String, String> platformSpec, TableProperties tableProperties) {
            return null;
        }

        @Override
        protected String getJarLocation() {
            return null;
        }
    }
    
    private static class TestTablePropertiesProvider extends TablePropertiesProvider {
        private final Schema schema;
        private final long splitThreshold;
        
        TestTablePropertiesProvider(Schema schema, long splitThreshold) {
            super(null, null);
            this.schema = schema;
            this.splitThreshold = splitThreshold;
        }
        
        TestTablePropertiesProvider(Schema schema) {
            this(schema, 1_000_000_000L);
        }
        
        @Override
        public TableProperties getTableProperties(String tableName) {
            if (tableName.equals("table-that-does-not-exist")) {
                return null;
            }
            TableProperties tableProperties = new TableProperties(new InstanceProperties());
            tableProperties.setSchema(schema);
            tableProperties.set(PARTITION_SPLIT_THRESHOLD, "" + splitThreshold);
            return tableProperties;
        }
    }
}