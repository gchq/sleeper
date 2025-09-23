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
package sleeper.core.properties.testutils;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.DataEngine;
import sleeper.core.properties.model.DefaultAsyncCommitBehaviour;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;

import static sleeper.core.properties.instance.ArrayListIngestProperty.MAX_IN_MEMORY_BATCH_SIZE;
import static sleeper.core.properties.instance.ArrayListIngestProperty.MAX_ROWS_TO_WRITE_LOCALLY;
import static sleeper.core.properties.instance.ArrowIngestProperty.ARROW_INGEST_BATCH_BUFFER_BYTES;
import static sleeper.core.properties.instance.ArrowIngestProperty.ARROW_INGEST_MAX_LOCAL_STORE_BYTES;
import static sleeper.core.properties.instance.ArrowIngestProperty.ARROW_INGEST_MAX_SINGLE_WRITE_TO_FILE_ROWS;
import static sleeper.core.properties.instance.ArrowIngestProperty.ARROW_INGEST_WORKING_BUFFER_BYTES;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_TRACKER_TABLE_NAME;
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
import static sleeper.core.properties.instance.IngestProperty.INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_ASYNC_COMMIT_BEHAVIOUR;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_DATA_ENGINE;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_MIN_TRANSACTIONS_AHEAD_TO_LOAD_SNAPSHOT;

/**
 * Helpers to create instance properties.
 */
public class InstancePropertiesTestHelper {

    private InstancePropertiesTestHelper() {
    }

    /**
     * Creates properties for a Sleeper instance. Generates a random instance ID and pre-populates various properties
     * set during deployment.
     *
     * @return the instance properties
     */
    public static InstanceProperties createTestInstanceProperties() {
        String id = UUID.randomUUID().toString().toLowerCase(Locale.ROOT).substring(0, 18);
        return createTestInstancePropertiesWithId(id);
    }

    /**
     * Creates properties for a Sleeper instance. Pre-populates various properties set during deployment.
     *
     * @param  id the instance ID
     * @return    the instance properties
     */
    public static InstanceProperties createTestInstancePropertiesWithId(String id) {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, id);
        instanceProperties.set(CONFIG_BUCKET, InstanceProperties.getConfigBucketFromInstanceId(id));
        instanceProperties.set(DATA_BUCKET, "sleeper-" + id + "-table-data");
        instanceProperties.set(JARS_BUCKET, "sleeper-" + id + "-jars");
        instanceProperties.set(QUERY_RESULTS_BUCKET, "sleeper-" + id + "-query-results");
        instanceProperties.set(ACCOUNT, "test-account");
        instanceProperties.set(REGION, "test-region");
        instanceProperties.set(VERSION, "1.2.3");
        instanceProperties.set(VPC_ID, "test-vpc");
        instanceProperties.set(SUBNETS, "test-subnet");
        instanceProperties.set(TRANSACTION_LOG_FILES_TABLENAME, "sleeper-" + id + "-file-transaction-log");
        instanceProperties.set(TRANSACTION_LOG_PARTITIONS_TABLENAME, "sleeper-" + id + "-partition-transaction-log");
        instanceProperties.set(TRANSACTION_LOG_ALL_SNAPSHOTS_TABLENAME, "sleeper-" + id + "-transaction-log-all-snapshots");
        instanceProperties.set(TRANSACTION_LOG_LATEST_SNAPSHOTS_TABLENAME, "sleeper-" + id + "-transaction-log-latest-snapshots");
        instanceProperties.set(TABLE_NAME_INDEX_DYNAMO_TABLENAME, "sleeper-" + id + "-table-index-by-name");
        instanceProperties.set(TABLE_ID_INDEX_DYNAMO_TABLENAME, "sleeper-" + id + "-table-index-by-id");
        instanceProperties.set(TABLE_ONLINE_INDEX_DYNAMO_TABLENAME, "sleeper-" + id + "-table-index-by-online");
        instanceProperties.set(QUERY_TRACKER_TABLE_NAME, "sleeper-" + id + "-query-tracking-table");
        instanceProperties.setNumber(MAXIMUM_CONNECTIONS_TO_S3, 5);
        instanceProperties.setNumber(DEFAULT_MIN_TRANSACTIONS_AHEAD_TO_LOAD_SNAPSHOT, 1);

        instanceProperties.setEnum(DEFAULT_ASYNC_COMMIT_BEHAVIOUR, DefaultAsyncCommitBehaviour.DISABLED);
        instanceProperties.setEnum(DEFAULT_DATA_ENGINE, DataEngine.JAVA);

        // Ingest
        instanceProperties.setNumber(INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS, Integer.MAX_VALUE);
        instanceProperties.setNumber(ARROW_INGEST_MAX_SINGLE_WRITE_TO_FILE_ROWS, 128);
        instanceProperties.setNumber(ARROW_INGEST_WORKING_BUFFER_BYTES, 16 * 1024 * 1024L);
        instanceProperties.setNumber(ARROW_INGEST_BATCH_BUFFER_BYTES, 16 * 1024 * 1024L);
        instanceProperties.setNumber(ARROW_INGEST_MAX_LOCAL_STORE_BYTES, 512 * 1024 * 1024L);
        instanceProperties.setNumber(MAX_ROWS_TO_WRITE_LOCALLY, 1000);
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
