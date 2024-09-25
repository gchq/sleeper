/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.configuration.properties;

import com.amazonaws.services.s3.AmazonS3;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.validation.DefaultAsyncCommitBehaviour;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;

import static sleeper.core.properties.instance.ArrayListIngestProperty.MAX_IN_MEMORY_BATCH_SIZE;
import static sleeper.core.properties.instance.ArrayListIngestProperty.MAX_RECORDS_TO_WRITE_LOCALLY;
import static sleeper.core.properties.instance.ArrowIngestProperty.ARROW_INGEST_BATCH_BUFFER_BYTES;
import static sleeper.core.properties.instance.ArrowIngestProperty.ARROW_INGEST_MAX_LOCAL_STORE_BYTES;
import static sleeper.core.properties.instance.ArrowIngestProperty.ARROW_INGEST_MAX_SINGLE_WRITE_TO_FILE_RECORDS;
import static sleeper.core.properties.instance.ArrowIngestProperty.ARROW_INGEST_WORKING_BUFFER_BYTES;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.ACTIVE_FILES_TABLELENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.FILE_REFERENCE_COUNT_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.PARTITION_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_TRACKER_TABLE_NAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REVISION_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_ID_INDEX_DYNAMO_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_NAME_INDEX_DYNAMO_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_ONLINE_INDEX_DYNAMO_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_ALL_SNAPSHOTS_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_FILES_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_LATEST_SNAPSHOTS_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_PARTITIONS_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.MAXIMUM_CONNECTIONS_TO_S3;
import static sleeper.core.properties.instance.CommonProperty.REGION;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;
import static sleeper.core.properties.instance.DefaultProperty.DEFAULT_ASYNC_COMMIT_BEHAVIOUR;
import static sleeper.core.properties.instance.DefaultProperty.DEFAULT_MIN_TRANSACTIONS_AHEAD_TO_LOAD_SNAPSHOT;
import static sleeper.core.properties.instance.IngestProperty.INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS;

/**
 * Helpers to create instance properties.
 */
public class InstancePropertiesTestHelper {

    private InstancePropertiesTestHelper() {
    }

    /**
     * Creates properties for a Sleeper instance and saves them to S3. Generates a random instance ID and pre-populates
     * various properties set during deployment.
     *
     * @param  s3 the S3 client
     * @return    the instance properties
     */
    public static InstanceProperties createTestInstanceProperties(AmazonS3 s3) {
        return createTestInstanceProperties(s3, properties -> {
        });
    }

    /**
     * Creates properties for a Sleeper instance and saves them to S3. Generates a random instance ID and pre-populates
     * various properties set during deployment.
     *
     * @param  s3              the S3 client
     * @param  extraProperties extra configuration to apply before saving to S3
     * @return                 the instance properties
     */
    public static InstanceProperties createTestInstanceProperties(
            AmazonS3 s3, Consumer<InstanceProperties> extraProperties) {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        extraProperties.accept(instanceProperties);
        try {
            s3.createBucket(instanceProperties.get(CONFIG_BUCKET));
            S3InstanceProperties.saveToS3(s3, instanceProperties);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to save instance properties", e);
        }
        return instanceProperties;
    }

    /**
     * Creates properties for a Sleeper instance. Generates a random instance ID and pre-populates various properties
     * set during deployment.
     *
     * @return the instance properties
     */
    public static InstanceProperties createTestInstanceProperties() {
        String id = UUID.randomUUID().toString().toLowerCase(Locale.ROOT).substring(0, 18);
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, id);
        instanceProperties.set(CONFIG_BUCKET, S3InstanceProperties.getConfigBucketFromInstanceId(id));
        instanceProperties.set(DATA_BUCKET, "test-data-bucket-" + id);
        instanceProperties.set(JARS_BUCKET, "test-bucket");
        instanceProperties.set(ACCOUNT, "test-account");
        instanceProperties.set(REGION, "test-region");
        instanceProperties.set(VERSION, "1.2.3");
        instanceProperties.set(VPC_ID, "test-vpc");
        instanceProperties.set(SUBNETS, "test-subnet");
        instanceProperties.set(ACTIVE_FILES_TABLELENAME, id + "-af");
        instanceProperties.set(FILE_REFERENCE_COUNT_TABLENAME, id + "-frc");
        instanceProperties.set(PARTITION_TABLENAME, id + "-p");
        instanceProperties.set(REVISION_TABLENAME, id + "-rv");
        instanceProperties.set(TRANSACTION_LOG_FILES_TABLENAME, id + "-ftl");
        instanceProperties.set(TRANSACTION_LOG_PARTITIONS_TABLENAME, id + "-ptl");
        instanceProperties.set(TRANSACTION_LOG_ALL_SNAPSHOTS_TABLENAME, id + "-tlas");
        instanceProperties.set(TRANSACTION_LOG_LATEST_SNAPSHOTS_TABLENAME, id + "-tlls");
        instanceProperties.set(TABLE_NAME_INDEX_DYNAMO_TABLENAME, id + "-tni");
        instanceProperties.set(TABLE_ID_INDEX_DYNAMO_TABLENAME, id + "-tii");
        instanceProperties.set(TABLE_ONLINE_INDEX_DYNAMO_TABLENAME, id + "-tio");
        instanceProperties.set(QUERY_TRACKER_TABLE_NAME, id + "-qt");
        instanceProperties.setNumber(MAXIMUM_CONNECTIONS_TO_S3, 5);
        instanceProperties.setNumber(DEFAULT_MIN_TRANSACTIONS_AHEAD_TO_LOAD_SNAPSHOT, 1);

        instanceProperties.set(DEFAULT_ASYNC_COMMIT_BEHAVIOUR, DefaultAsyncCommitBehaviour.DISABLED.toString());

        // Ingest
        instanceProperties.setNumber(INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS, Integer.MAX_VALUE);
        instanceProperties.setNumber(ARROW_INGEST_MAX_SINGLE_WRITE_TO_FILE_RECORDS, 128);
        instanceProperties.setNumber(ARROW_INGEST_WORKING_BUFFER_BYTES, 16 * 1024 * 1024L);
        instanceProperties.setNumber(ARROW_INGEST_BATCH_BUFFER_BYTES, 16 * 1024 * 1024L);
        instanceProperties.setNumber(ARROW_INGEST_MAX_LOCAL_STORE_BYTES, 512 * 1024 * 1024L);
        instanceProperties.setNumber(MAX_RECORDS_TO_WRITE_LOCALLY, 1000);
        instanceProperties.setNumber(MAX_IN_MEMORY_BATCH_SIZE, 100000);
        return instanceProperties;
    }

    /**
     * Writes a properties file as a string.
     *
     * @param  properties the properties
     * @return            the properties file contents
     */
    public static String propertiesString(Properties properties) throws IOException {
        StringWriter writer = new StringWriter();
        properties.store(writer, "");
        return writer.toString();
    }
}
