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
        createLogGroup("statestore-committer");

        // Accessed via CoreStacks getters
        createLogGroup("state-snapshot-creation-trigger");
        createLogGroup("state-snapshot-creation");
        createLogGroup("state-transaction-deletion-trigger");
        createLogGroup("state-transaction-deletion");
        createLogGroup("metrics-trigger");
        createLogGroup("metrics-publisher");
        createLogGroup("bulk-import-EMRServerless-start");
        createLogGroup("bulk-import-NonPersistentEMR-start");
        createLogGroup("bulk-import-PersistentEMR-start");
        createLogGroup("bulk-import-eks-starter");
        createLogGroup("IngestTasks");
        createLogGroup("FargateCompactionTasks");
        createLogGroup("EC2CompactionTasks");
        createLogGroup("garbage-collector-trigger");
        createLogGroup("garbage-collector");
        createLogGroup("Simple-athena-handler");
        createLogGroup("IteratorApplying-athena-handler");
    }

    public ILogGroup getLogGroupByFunctionName(String functionName) {
        return getLogGroupByNameWithPrefixes(functionName);
    }

    public ILogGroup getLogGroupByECSLogDriverId(String id) {
        return getLogGroupByNameWithPrefixes(addNamePrefixes(id));
    }

    private ILogGroup getLogGroupByNameWithPrefixes(String nameWithPrefixes) {
        return Objects.requireNonNull(logGroupByName.get(nameWithPrefixes), "No log group found: " + nameWithPrefixes);
    }

    private void createLogGroup(String logGroupName) {
        String nameWithPrefixes = addNamePrefixes(logGroupName);
        logGroupByName.put(nameWithPrefixes, LogGroup.Builder.create(this, logGroupName)
                .logGroupName(addNamePrefixes(nameWithPrefixes))
                .retention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS)))
                .build());
    }

    private String addNamePrefixes(String id) {
        return String.join("-", "sleeper", Utils.cleanInstanceId(instanceProperties), id);
    }
}
