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
package sleeper.core.deploy;

import sleeper.core.properties.instance.InstanceProperty;

import java.util.List;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_CREATION_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_CREATION_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_PENDING_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_PENDING_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.FIND_PARTITIONS_TO_SPLIT_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.FIND_PARTITIONS_TO_SPLIT_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.GARBAGE_COLLECTOR_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.GARBAGE_COLLECTOR_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BATCHER_SUBMIT_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BATCHER_SUBMIT_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.LEAF_PARTITION_QUERY_QUEUE_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.LEAF_PARTITION_QUERY_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_JOB_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_METRICS_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_METRICS_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_SNAPSHOT_CREATION_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_SNAPSHOT_CREATION_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_SNAPSHOT_DELETION_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_SNAPSHOT_DELETION_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_TRANSACTION_DELETION_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_TRANSACTION_DELETION_QUEUE_URL;

/**
 * Definitions of SQS queues deployed in a Sleeper instance. Used when purging all queues in an instance.
 */
public class SqsQueues {

    private SqsQueues() {
    }

    public static final List<InstanceProperty> QUEUE_PROPERTIES = List.of(
            TRANSACTION_LOG_SNAPSHOT_CREATION_QUEUE_URL,
            TRANSACTION_LOG_SNAPSHOT_DELETION_QUEUE_URL,
            TRANSACTION_LOG_TRANSACTION_DELETION_QUEUE_URL,
            STATESTORE_COMMITTER_QUEUE_URL,
            TABLE_METRICS_QUEUE_URL,
            QUERY_QUEUE_URL,
            QUERY_RESULTS_QUEUE_URL,
            LEAF_PARTITION_QUERY_QUEUE_URL,
            COMPACTION_JOB_CREATION_QUEUE_URL,
            COMPACTION_JOB_QUEUE_URL,
            COMPACTION_PENDING_QUEUE_URL,
            FIND_PARTITIONS_TO_SPLIT_QUEUE_URL,
            PARTITION_SPLITTING_JOB_QUEUE_URL,
            GARBAGE_COLLECTOR_QUEUE_URL,
            INGEST_JOB_QUEUE_URL,
            INGEST_BATCHER_SUBMIT_QUEUE_URL,
            BULK_IMPORT_EMR_JOB_QUEUE_URL,
            BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL,
            BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL,
            BULK_IMPORT_EKS_JOB_QUEUE_URL);

    public static final List<InstanceProperty> DEAD_LETTER_QUEUE_PROPERTIES = List.of(
            TRANSACTION_LOG_SNAPSHOT_CREATION_DLQ_URL,
            TRANSACTION_LOG_SNAPSHOT_DELETION_DLQ_URL,
            TRANSACTION_LOG_TRANSACTION_DELETION_DLQ_URL,
            STATESTORE_COMMITTER_DLQ_URL,
            TABLE_METRICS_DLQ_URL,
            QUERY_DLQ_URL,
            LEAF_PARTITION_QUERY_QUEUE_DLQ_URL,
            COMPACTION_JOB_CREATION_DLQ_URL,
            COMPACTION_JOB_DLQ_URL,
            COMPACTION_PENDING_DLQ_URL,
            FIND_PARTITIONS_TO_SPLIT_DLQ_URL,
            PARTITION_SPLITTING_JOB_DLQ_URL,
            GARBAGE_COLLECTOR_DLQ_URL,
            INGEST_JOB_DLQ_URL,
            INGEST_BATCHER_SUBMIT_DLQ_URL);

}
