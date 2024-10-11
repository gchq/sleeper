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
package sleeper.cdk.stack;

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
        createLogGroup("EksBulkImportStateMachine");
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
        return getLogGroupByNameWithPrefixes(addNamePrefixes(id));
    }

    private ILogGroup getLogGroupByNameWithPrefixes(String nameWithPrefixes) {
        return Objects.requireNonNull(logGroupByName.get(nameWithPrefixes), "No log group found: " + nameWithPrefixes);
    }

    private void createLogGroup(String shortName) {
        String nameWithPrefixes = addNamePrefixes(shortName);
        logGroupByName.put(nameWithPrefixes, LogGroup.Builder.create(this, shortName)
                .logGroupName(nameWithPrefixes)
                .retention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS)))
                .build());
    }

    private String addNamePrefixes(String shortName) {
        return String.join("-", "sleeper", Utils.cleanInstanceId(instanceProperties), shortName);
    }
}
