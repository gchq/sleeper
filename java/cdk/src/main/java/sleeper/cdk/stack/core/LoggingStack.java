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

    private final Map<String, ILogGroup> logGroupByName = new HashMap<>();
    private final InstanceProperties instanceProperties;

    public LoggingStack(Construct scope, String id, InstanceProperties instanceProperties) {
        super(scope, id);
        this.instanceProperties = instanceProperties;

        // Accessed directly by getter on this class
        createLogGroup("vpc-check");
        createLogGroup("vpc-check-provider");
        createLogGroup("config-autodelete");
        createLogGroup("config-autodelete-provider");
        createLogGroup("table-data-autodelete");
        createLogGroup("table-data-autodelete-provider");
        createLogGroup("statestore-committer");

        // Accessed via CoreStacks getters
        createLogGroup("properties-writer");
        createLogGroup("properties-writer-provider");
        createLogGroup("state-snapshot-creation-trigger");
        createLogGroup("state-snapshot-creation");
        createLogGroup("state-snapshot-deletion-trigger");
        createLogGroup("state-snapshot-deletion");
        createLogGroup("state-transaction-deletion-trigger");
        createLogGroup("state-transaction-deletion");
        createLogGroup("metrics-trigger");
        createLogGroup("metrics-publisher");
        createLogGroup("bulk-import-EMRServerless-start");
        createLogGroup("bulk-import-NonPersistentEMR-start");
        createLogGroup("bulk-import-PersistentEMR-start");
        createLogGroup("bulk-import-eks-starter");
        createLogGroup("bulk-import-eks");
        createStateMachineLogGroup("EksBulkImportStateMachine");
        createLogGroup("bulk-import-autodelete");
        createLogGroup("bulk-import-autodelete-provider");
        createLogGroup("IngestTasks");
        createLogGroup("ingest-create-tasks");
        createLogGroup("ingest-batcher-submit-files");
        createLogGroup("ingest-batcher-create-jobs");
        createLogGroup("partition-splitting-trigger");
        createLogGroup("partition-splitting-find-to-split");
        createLogGroup("partition-splitting-handler");
        createLogGroup("FargateCompactionTasks");
        createLogGroup("EC2CompactionTasks");
        createLogGroup("compaction-job-creation-trigger");
        createLogGroup("compaction-job-creation-handler");
        createLogGroup("compaction-tasks-creator");
        createLogGroup("compaction-custom-termination");
        createLogGroup("garbage-collector-trigger");
        createLogGroup("garbage-collector");
        createLogGroup("query-executor");
        createLogGroup("query-leaf-partition");
        createLogGroup("query-websocket-handler");
        createLogGroup("query-results-autodelete");
        createLogGroup("query-results-autodelete-provider");
        createLogGroup("query-keep-warm");
        createLogGroup("Simple-athena-handler");
        createLogGroup("IteratorApplying-athena-handler");
        createLogGroup("spill-bucket-autodelete");
        createLogGroup("spill-bucket-autodelete-provider");
    }

    public ILogGroup getLogGroupByFunctionName(String functionName) {
        return getLogGroupByNameWithPrefixes(functionName);
    }

    public ILogGroup getProviderLogGroupByFunctionName(String functionName) {
        return getLogGroupByNameWithPrefixes(functionName + "-provider");
    }

    public ILogGroup getLogGroupByECSLogDriverId(String id) {
        return getLogGroupByNameWithPrefixes(addNamePrefixes(id));
    }

    public ILogGroup getLogGroupByStateMachineId(String id) {
        return getLogGroupByNameWithPrefixes(addStateMachineNamePrefixes(id));
    }

    public ILogGroup getLogGroupByEksClusterName(String clusterName) {
        return getLogGroupByNameWithPrefixes(clusterName);
    }

    private ILogGroup getLogGroupByNameWithPrefixes(String nameWithPrefixes) {
        return Objects.requireNonNull(logGroupByName.get(nameWithPrefixes), "No log group found: " + nameWithPrefixes);
    }

    private void createLogGroup(String shortName) {
        createLogGroup(shortName, addNamePrefixes(shortName));
    }

    private void createStateMachineLogGroup(String shortName) {
        createLogGroup(shortName, addStateMachineNamePrefixes(shortName));
    }

    private void createLogGroup(String shortName, String nameWithPrefixes) {
        logGroupByName.put(nameWithPrefixes, LogGroup.Builder.create(this, shortName)
                .logGroupName(nameWithPrefixes)
                .retention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS)))
                .build());
    }

    private String addStateMachineNamePrefixes(String shortName) {
        return "/aws/vendedlogs/states/" + addNamePrefixes(shortName);
    }

    private String addNamePrefixes(String shortName) {
        return String.join("-", "sleeper", Utils.cleanInstanceId(instanceProperties), shortName);
    }

    public enum LoggingGroupName {

        VPC_CHECK("vpc-check"),
        VPC_CHECK_PROVIDER("vpc-check-provider"),
        CONFIG_AUTODELETE("config-autodelete"),
        CONFIG_AUTODELETE_PROVIDER("config-autodelete-provider"),
        TABLE_DATA_AUTODELETE("table-data-autodelete"),
        TABLE_DATA_AUTODELETE_PROVIDER("table-data-autodelete-provider"),
        STATESTORE_COMMITTER("statestore-committer"),

        // Accessed via CoreStacks getters
        PROPERTIES_WRITER("properties-writer"),
        PROPERTIES_WRITER_PROVIDER("properties-writer-provider"),
        STATE_SNAPSHOT_CREATION_TRIGGER("state-snapshot-creation-trigger"),
        STATE_SNAPSHOT_CREATION("state-snapshot-creation"),
        STATE_SNAPSHOT_DELETION_TRIGGER("state-snapshot-deletion-trigger"),
        STATE_SNAPSHOT_DELETION("state-snapshot-deletion"),
        STATE_TRANSACTION_DELETION_TRIGGER("state-transaction-deletion-trigger"),
        STATE_TRANSACTION_DELETION("state-transaction-deletion"),
        METRICS_TRIGGER("metrics-trigger"),
        METRICS_PUBLISHER("metrics-publisher"),
        BULK_IMPORT_EMRSERVERLESS_START("bulk-import-EMRServerless-start"),
        BULK_IMPORT_NONPERSISTENTEMR_START("bulk-import-NonPersistentEMR-start"),
        BULK_IMPORT_PERSISTENTEMR_START("bulk-import-PersistentEMR-start"),
        BULK_IMPORT_EKS_STARTER("bulk-import-eks-starter"),
        BULK_IMPORT_EKS("bulk-import-eks"),
        EKSBULKIMPORTSTATEMACHINE("EksBulkImportStateMachine", "/aws/vendedlogs/states/"),
        BULK_IMPORT_AUTODELETE("bulk-import-autodelete"),
        BULK_IMPORT_AUTODELETE_PROVIDER("bulk-import-autodelete-provider"),
        INGEST_TASKS("IngestTasks"),
        INGEST_CREATE_TASKS("ingest-create-tasks"),
        INGEST_BATCHER_SUBMIT_FILES("ingest-batcher-submit-files"),
        INGEST_BATCHER_CREATE_JOBS("ingest-batcher-create-jobs"),
        PARTITION_SPLITTING_TRIGGER("partition-splitting-trigger"),
        PARTITION_SPLITTING_FIND_TO_SPLIT("partition-splitting-find-to-split"),
        PARTITION_SPLITTING_HANDLER("partition-splitting-handler"),
        FARGATECOMPACTIONTASKS("FargateCompactionTasks"),
        EC2COMPACTIONTASKS("EC2CompactionTasks"),
        COMPACTION_JOB_CREATION_TRIGGER("compaction-job-creation-trigger"),
        COMPACTION_JOB_CREATION_HANDLER("compaction-job-creation-handler"),
        COMPACTION_TASKS_CREATOR("compaction-tasks-creator"),
        COMPACTION_CUSTOM_TERMINATION("compaction-custom-termination"),
        GARBAGE_COLLECTOR_TRIGGER("garbage-collector-trigger"),
        GARBAGE_COLLECTOR("garbage-collector"),
        QUERY_EXECUTOR("query-executor"),
        QUERY_LEAF_PARTITION("query-leaf-partition"),
        QUERY_WEBSOCKET_HANDLER("query-websocket-handler"),
        QUERY_RESULTS_AUTODELETE("query-results-autodelete"),
        QUERY_RESULTS_AUTODELETE_PROVIDER("query-results-autodelete-provider"),
        QUERY_KEEP_WARM("query-keep-warm"),
        SIMPLE_ATHENA_HANDLER("Simple-athena-handler"),
        ITERATORAPPLYING_ATHENA_HANDLER("IteratorApplying-athena-handler"),
        SPILL_BUCKET_AUTODELETE("spill-bucket-autodelete"),
        SPILL_BUCKET_AUTODELETE_PROVIDER("spill-bucket-autodelete-provider");

        LoggingGroupName(String shortName) {
        }

        LoggingGroupName(String shortName, String prefix) {
        }

    }
}
