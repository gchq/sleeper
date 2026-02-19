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
package sleeper.cdk.stack.core;

import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.logs.ILogGroup;
import software.amazon.awscdk.services.logs.LogGroup;
import software.constructs.Construct;

import sleeper.cdk.util.Utils;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static sleeper.core.properties.instance.CommonProperty.LOG_RETENTION_IN_DAYS;

public class LoggingStack extends NestedStack {

    private final Map<LogGroupRef, ILogGroup> logGroupByRef = new HashMap<>();

    public LoggingStack(Construct scope, String id, InstanceProperties instanceProperties) {
        super(scope, id);

        for (LogGroupRef ref : LogGroupRef.values()) {
            logGroupByRef.put(ref, createLogGroup(this, ref, instanceProperties));
        }
        Utils.addTags(this, instanceProperties);
    }

    public ILogGroup getLogGroup(LogGroupRef ref) {
        return Objects.requireNonNull(logGroupByRef.get(ref), "No log group found: " + ref);
    }

    /**
     * Creates a log group for a standalone deployment where the logging stack is not deployed.
     * When deploying a Sleeper instance, call {@link #getLogGroup} instead.
     *
     * @param  scope              the scope to add the log group to
     * @param  ref                the log group to deploy
     * @param  instanceProperties the fake instance properties, usually adapted from system test properties
     * @return                    the log group
     */
    public static ILogGroup createLogGroup(Construct scope, LogGroupRef ref, InstanceProperties instanceProperties) {
        return LogGroup.Builder.create(scope, ref.shortName)
                .logGroupName(ref.prefix + String.join("-", "sleeper", Utils.cleanInstanceId(instanceProperties), ref.shortName))
                .retention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS)))
                .removalPolicy(Utils.logsRemovalPolicy(instanceProperties))
                .build();
    }

    public enum LogGroupRef {

        // Accessed directly by getter on this class
        VPC_CHECK("vpc-check"),
        VPC_CHECK_PROVIDER("vpc-check-provider"),
        STATESTORE_COMMITTER("statestore-committer"),

        // Accessed via CoreStacks getters
        PROPERTIES_WRITER("properties-writer"),
        PROPERTIES_WRITER_PROVIDER("properties-writer-provider"),
        TABLE_DEFINER("table-definer"),
        TABLE_DEFINER_PROVIDER("table-definer-provider"),
        STATE_SNAPSHOT_CREATION_TRIGGER("state-snapshot-creation-trigger"),
        STATE_SNAPSHOT_CREATION("state-snapshot-creation"),
        STATE_SNAPSHOT_DELETION_TRIGGER("state-snapshot-deletion-trigger"),
        STATE_SNAPSHOT_DELETION("state-snapshot-deletion"),
        STATE_TRANSACTION_DELETION_TRIGGER("state-transaction-deletion-trigger"),
        STATE_TRANSACTION_DELETION("state-transaction-deletion"),
        STATE_TRANSACTION_FOLLOWER("state-transaction-follower"),
        METRICS_TRIGGER("metrics-trigger"),
        METRICS_PUBLISHER("metrics-publisher"),
        AUTO_DELETE_S3_OBJECTS("auto-delete-s3-objects"),
        AUTO_DELETE_S3_OBJECTS_PROVIDER("auto-delete-s3-objects-provider"),
        AUTO_STOP_ECS_CLUSTER_TASKS("auto-stop-ecs-cluster-tasks"),
        AUTO_STOP_ECS_CLUSTER_TASKS_PROVIDER("auto-stop-ecs-cluster-tasks-provider"),
        AUTO_STOP_EMR_SERVERLESS_APPLICATION("auto-stop-emr-serverless"),
        AUTO_STOP_EMR_SERVERLESS_APPLICATION_PROVIDER("auto-stop-emr-serverless-provider"),
        BULK_EXPORT("bulk-export"),
        BULK_EXPORT_TASKS("FargateBulkExportTasks"),
        BULK_EXPORT_TASKS_CREATOR("bulk-export-task-creator"),
        BULK_IMPORT_EMR_SERVERLESS_START("bulk-import-EMRServerless-start"),
        BULK_IMPORT_EMR_NON_PERSISTENT_START("bulk-import-NonPersistentEMR-start"),
        BULK_IMPORT_EMR_PERSISTENT_START("bulk-import-PersistentEMR-start"),
        BULK_IMPORT_EKS_STARTER("bulk-import-eks-starter"),
        BULK_IMPORT_EKS("bulk-import-eks"),
        BULK_IMPORT_EKS_STATE_MACHINE("EksBulkImportStateMachine", "/aws/vendedlogs/states/"),
        INGEST_TASKS("IngestTasks"),
        INGEST_CREATE_TASKS("ingest-create-tasks"),
        INGEST_BATCHER_SUBMIT_FILES("ingest-batcher-submit-files"),
        INGEST_BATCHER_CREATE_JOBS("ingest-batcher-create-jobs"),
        PARTITION_SPLITTING_TRIGGER("partition-splitting-trigger"),
        PARTITION_SPLITTING_FIND_TO_SPLIT("partition-splitting-find-to-split"),
        PARTITION_SPLITTING_HANDLER("partition-splitting-handler"),
        COMPACTION_TASKS_FARGATE("FargateCompactionTasks"),
        COMPACTION_TASKS_EC2("EC2CompactionTasks"),
        COMPACTION_JOB_CREATION_TRIGGER("compaction-job-creation-trigger"),
        COMPACTION_JOB_CREATION_HANDLER("compaction-job-creation-handler"),
        COMPACTION_JOB_DISPATCHER("compaction-job-dispatcher"),
        COMPACTION_TASKS_CREATOR("compaction-tasks-creator"),
        COMPACTION_CUSTOM_TERMINATION("compaction-custom-termination"),
        COMPACTION_COMMIT_BATCHER("compaction-commit-batcher"),
        GARBAGE_COLLECTOR_TRIGGER("garbage-collector-trigger"),
        GARBAGE_COLLECTOR("garbage-collector"),
        QUERY_EXECUTOR("query-executor"),
        QUERY_LEAF_PARTITION("query-leaf-partition"),
        QUERY_WEBSOCKET_HANDLER("query-websocket-handler"),
        QUERY_KEEP_WARM("query-keep-warm"),
        SIMPLE_ATHENA_HANDLER("Simple-athena-handler"),
        ITERATOR_APPLYING_ATHENA_HANDLER("IteratorApplying-athena-handler");

        private final String shortName;
        private final String prefix;

        LogGroupRef(String shortName) {
            this(shortName, "");
        }

        LogGroupRef(String shortName, String prefix) {
            this.shortName = shortName;
            this.prefix = prefix;
        }
    }
}
