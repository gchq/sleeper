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

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.InstanceProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_CREATION_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_TASK_CREATION_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.GARBAGE_COLLECTOR_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_BATCHER_JOB_CREATION_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.QUERY_WARM_LAMBDA_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TABLE_METRICS_RULE;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_SNAPSHOT_CREATION_RULE;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_SNAPSHOT_DELETION_RULE;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_TRANSACTION_DELETION_RULE;
import static sleeper.configuration.properties.instance.CommonProperty.ID;

public class SleeperScheduleRule {

    private static final List<SleeperScheduleRule> RULES = new ArrayList<>();
    // Rule that creates compaction jobs
    public static final SleeperScheduleRule COMPACTION_JOB_CREATION = add(
            COMPACTION_JOB_CREATION_CLOUDWATCH_RULE, "%s-CompactionJobCreationRule");
    // Rule that creates compaction tasks
    public static final SleeperScheduleRule COMPACTION_TASK_CREATION = add(
            COMPACTION_TASK_CREATION_CLOUDWATCH_RULE, "%s-CompactionTasksCreationRule");
    // Rule that looks for partitions that need splitting
    public static final SleeperScheduleRule PARTITION_SPLITTING = add(
            PARTITION_SPLITTING_CLOUDWATCH_RULE, "%s-FindPartitionsToSplitPeriodicTrigger");
    // Rule that triggers garbage collector lambda
    public static final SleeperScheduleRule GARBAGE_COLLECTOR = add(
            GARBAGE_COLLECTOR_CLOUDWATCH_RULE, "%s-GarbageCollectorPeriodicTrigger");
    // Rule that triggers creation of ingest tasks
    public static final SleeperScheduleRule INGEST = add(
            INGEST_CLOUDWATCH_RULE, "%s-IngestTasksCreationRule");
    // Rule that batches up ingest jobs from file ingest requests
    public static final SleeperScheduleRule INGEST_BATCHER_JOB_CREATION = add(
            INGEST_BATCHER_JOB_CREATION_CLOUDWATCH_RULE, "%s-IngestBatcherJobCreationRule");
    public static final SleeperScheduleRule TABLE_METRICS = add(TABLE_METRICS_RULE, "%s-MetricsPublishRule");
    // Rule that triggers the query lambdas to keep warm
    public static final SleeperScheduleRule QUERY_WARM_LAMBDA = add(
            QUERY_WARM_LAMBDA_CLOUDWATCH_RULE, "%s-QueryWarmLambdaRule");
    // Rule that triggers transaction log snapshot creation
    public static final SleeperScheduleRule TRANSACTION_LOG_SNAPSHOT_CREATION = add(
            TRANSACTION_LOG_SNAPSHOT_CREATION_RULE, "%s-TransactionLogSnapshotCreationRule");
    // Rule that triggers deletion of old transaction log snapshots
    public static final SleeperScheduleRule TRANSACTION_LOG_SNAPSHOT_DELETION = add(
            TRANSACTION_LOG_SNAPSHOT_DELETION_RULE, "%s-TransactionLogSnapshotDeletionRule");
    // Rule that triggers deletion of old transaction log transactions
    public static final SleeperScheduleRule TRANSACTION_LOG_TRANSACTION_DELETION = add(
            TRANSACTION_LOG_TRANSACTION_DELETION_RULE, "%s-TransactionLogTransactionDeletionRule");

    private final InstanceProperty property;
    private final String nameFormat;

    private static SleeperScheduleRule add(InstanceProperty property, String nameFormat) {
        SleeperScheduleRule rule = new SleeperScheduleRule(property, nameFormat);
        RULES.add(rule);
        return rule;
    }

    private SleeperScheduleRule(InstanceProperty property, String nameFormat) {
        this.property = requireNonNull(property, "property must not be null");
        this.nameFormat = requireNonNull(nameFormat, "nameFormat must not be null");
    }

    public static Stream<Value> getCloudWatchRules(InstanceProperties properties) {
        return RULES.stream()
                .map(rule -> rule.readValue(properties));
    }

    public static Stream<Value> getCloudWatchRuleDefaults(String instanceId) {
        return RULES.stream()
                .map(rule -> rule.getDefault(instanceId));
    }

    public Value readValue(InstanceProperties properties) {
        return new Value(properties.get(property));
    }

    public String buildRuleName(InstanceProperties properties) {
        return buildRuleName(properties.get(ID));
    }

    public Value getDefault(String instanceId) {
        return new Value(buildRuleName(instanceId));
    }

    public String buildRuleName(String instanceId) {
        String name = String.format(nameFormat, instanceId);
        if (name.length() > 64) {
            return name.substring(0, 64);
        } else {
            return name;
        }
    }

    public InstanceProperty getProperty() {
        return property;
    }

    public class Value {
        private final String propertyValue;

        private Value(String propertyValue) {
            this.propertyValue = propertyValue;
        }

        public List<String> getRuleNames() {
            if (propertyValue == null || propertyValue.isBlank()) {
                return Collections.emptyList();
            } else {
                return List.of(propertyValue.split(","));
            }
        }

        public String getPropertyValue() {
            return propertyValue;
        }

        public InstanceProperty getProperty() {
            return property;
        }
    }

}
