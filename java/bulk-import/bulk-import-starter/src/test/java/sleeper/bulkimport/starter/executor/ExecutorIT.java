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
package sleeper.bulkimport.starter.executor;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.CommonTestConstants;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

@Testcontainers
class ExecutorIT {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3);

    private AmazonS3 createS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3))
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .build();
    }

    private static final Schema SCHEMA = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();

    @Test
    void shouldCallRunOnPlatformIfJobIsValid() {
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
        ExecutorMock executorMock = buildExecutorWithBulkImportBucketAndTable(bucketName, "myTable", s3);

        // When
        executorMock.runJob(importJob);

        // Then
        assertThat(executorMock.isRunJobOnPlatformCalled()).isTrue();
        s3.shutdown();
    }

    @Test
    void shouldFailIfFileListIsEmpty() {
        // Given
        AmazonS3 s3 = createS3Client();
        String bucketName = UUID.randomUUID().toString();
        s3.createBucket(bucketName);
        BulkImportJob importJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .files(Lists.newArrayList())
                .build();
        ExecutorMock executorMock = buildExecutorWithBulkImportBucketAndTable(bucketName, "myTable", s3);

        // When
        assertThatThrownBy(() -> executorMock.runJob(importJob))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(getExpectedErrorMessage("The input files must be set to a non-null and non-empty value."));
        s3.shutdown();
    }

    @Test
    void shouldSucceedIfS3ObjectIsADirectoryContainingFiles() {
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
        ExecutorMock executorMock = buildExecutorWithBulkImportBucketAndTable(bucketName, "myTable", s3);

        // When
        executorMock.runJob(importJob);

        // Then
        assertThat(executorMock.isRunJobOnPlatformCalled()).isTrue();
        s3.shutdown();
    }

    @Test
    void shouldFailIfJobPointsAtNonExistentTable() {
        // Given
        AmazonS3 s3 = createS3Client();
        String bucketName = UUID.randomUUID().toString();
        s3.createBucket(bucketName);
        BulkImportJob importJob = new BulkImportJob.Builder()
                .tableName("table-that-does-not-exist")
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();
        ExecutorMock executorMock = buildExecutorWithBulkImportBucketAndTable(bucketName, "myTable", s3);

        // When / Then
        assertThatThrownBy(() -> executorMock.runJob(importJob))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(getExpectedErrorMessage("Table does not exist."));
        s3.shutdown();
    }

    @Test
    void shouldThrowExceptionIfJobIdContainsMoreThan63Characters() {
        // Given
        String invalidId = UUID.randomUUID().toString() + UUID.randomUUID();
        BulkImportJob importJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .files(Lists.newArrayList("file1.parquet"))
                .id(invalidId)
                .build();
        ExecutorMock executorMock = buildExecutorWithTable("myTable");

        // When / Then
        assertThatThrownBy(() -> executorMock.runJob(importJob))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(getExpectedErrorMessage("Job IDs are only allowed to be up to 63 characters long."));
    }

    @Test
    void shouldThrowExceptionIfTableNameIsNull() {
        // Given
        BulkImportJob importJob = new BulkImportJob.Builder()
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();
        ExecutorMock executorMock = buildExecutorWithTable("myTable");

        // When / Then
        assertThatThrownBy(() -> executorMock.runJob(importJob))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(getExpectedErrorMessage("The table name must be set to a non-null value."));
    }

    @Test
    void shouldThrowExceptionIfJobIdContainsUppercaseLetters() {
        // Given
        BulkImportJob importJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("importJob")
                .files(Lists.newArrayList("file1.parquet"))
                .build();
        ExecutorMock executorMock = buildExecutorWithTable("myTable");

        // When / Then
        assertThatThrownBy(() -> executorMock.runJob(importJob))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(getExpectedErrorMessage("Job Ids must only contain lowercase alphanumerics and dashes."));
    }

    @Test
    void shouldDoNothingWhenJobIsNull() {
        // Given
        ExecutorMock executorMock = buildExecutorWithTable("myTable");

        // When
        executorMock.runJob(null);

        // Then
        assertThat(executorMock.isRunJobOnPlatformCalled()).isFalse();
    }

    private String getExpectedErrorMessage(String message) {
        return "The bulk import job failed validation with the following checks failing: \n"
                + message;
    }

    private ExecutorMock buildExecutorWithBulkImportBucketAndTable(String bucketName, String tableName, AmazonS3 s3) {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(BULK_IMPORT_BUCKET, bucketName);
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.setSchema(SCHEMA);
        TablePropertiesProvider tablePropertiesProvider = new FixedTablePropertiesProvider(tableProperties);
        return new ExecutorMock(instanceProperties, tablePropertiesProvider, s3);
    }

    private ExecutorMock buildExecutorWithTable(String tableName) {
        InstanceProperties instanceProperties = new InstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.setSchema(SCHEMA);
        TablePropertiesProvider tablePropertiesProvider = new FixedTablePropertiesProvider(tableProperties);
        return new ExecutorMock(instanceProperties, tablePropertiesProvider, null);
    }

    private static class ExecutorMock extends Executor {
        private boolean runJobOnPlatformCalled = false;

        public boolean isRunJobOnPlatformCalled() {
            return runJobOnPlatformCalled;
        }

        ExecutorMock(InstanceProperties instanceProperties,
                     TablePropertiesProvider tablePropertiesProvider,
                     AmazonS3 s3) {
            super(instanceProperties, tablePropertiesProvider, s3);
        }

        @Override
        protected boolean runJobOnPlatform(BulkImportJob bulkImportJob) {
            runJobOnPlatformCalled = true;
            return true;
        }

        @Override
        protected String getJarLocation() {
            return null;
        }
    }

}
