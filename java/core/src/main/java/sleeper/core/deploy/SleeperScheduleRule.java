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

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.instance.InstanceProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_CREATION_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_TASK_CREATION_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.GARBAGE_COLLECTOR_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BATCHER_JOB_CREATION_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_WARM_LAMBDA_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_METRICS_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_SNAPSHOT_CREATION_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_SNAPSHOT_DELETION_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_TRANSACTION_DELETION_RULE;
import static sleeper.core.properties.instance.CommonProperty.ID;

/**
 * Tracks properties for the names of CloudWatch rules that invoke various operations on a schedule. Used to pause and
 * resume the system.
 */
public class SleeperScheduleRule {

    private static final List<SleeperScheduleRule> RULES = new ArrayList<>();
    public static final SleeperScheduleRule COMPACTION_JOB_CREATION = add(
            COMPACTION_JOB_CREATION_CLOUDWATCH_RULE, "%s-CompactionJobCreationRule",
            "Triggers creation of compaction jobs for online Sleeper tables");
    public static final SleeperScheduleRule COMPACTION_TASK_CREATION = add(
            COMPACTION_TASK_CREATION_CLOUDWATCH_RULE, "%s-CompactionTasksCreationRule",
            "Triggers scaling compaction tasks to run created jobs");
    public static final SleeperScheduleRule PARTITION_SPLITTING = add(
            PARTITION_SPLITTING_CLOUDWATCH_RULE, "%s-FindPartitionsToSplitPeriodicTrigger",
            "Triggers looking for partitions to split in online Sleeper tables");
    public static final SleeperScheduleRule GARBAGE_COLLECTOR = add(
            GARBAGE_COLLECTOR_CLOUDWATCH_RULE, "%s-GarbageCollectorPeriodicTrigger",
            "Triggers garbage collection to delete unused files");
    public static final SleeperScheduleRule INGEST_TASK_CREATION = add(
            INGEST_CLOUDWATCH_RULE, "%s-IngestTasksCreationRule",
            "Triggers scaling ingest tasks to run queued jobs");
    public static final SleeperScheduleRule INGEST_BATCHER_JOB_CREATION = add(
            INGEST_BATCHER_JOB_CREATION_CLOUDWATCH_RULE, "%s-IngestBatcherJobCreationRule",
            "Triggers creation of jobs from files submitted to the ingest batcher");
    public static final SleeperScheduleRule TABLE_METRICS = add(TABLE_METRICS_RULE, "%s-MetricsPublishRule",
            "Triggers publishing metrics based on the current state of Sleeper tables");
    public static final SleeperScheduleRule QUERY_WARM_LAMBDA = add(
            QUERY_WARM_LAMBDA_CLOUDWATCH_RULE, "%s-QueryWarmLambdaRule",
            "Triggers query requests to prevent the system scaling to zero");
    public static final SleeperScheduleRule TRANSACTION_LOG_SNAPSHOT_CREATION = add(
            TRANSACTION_LOG_SNAPSHOT_CREATION_RULE, "%s-TransactionLogSnapshotCreationRule",
            "Triggers creation of snapshots of the current state of online Sleeper tables based on a transaction log");
    public static final SleeperScheduleRule TRANSACTION_LOG_SNAPSHOT_DELETION = add(
            TRANSACTION_LOG_SNAPSHOT_DELETION_RULE, "%s-TransactionLogSnapshotDeletionRule",
            "Triggers deletion of old snapshots of online Sleeper tables based on a transaction log");
    public static final SleeperScheduleRule TRANSACTION_LOG_TRANSACTION_DELETION = add(
            TRANSACTION_LOG_TRANSACTION_DELETION_RULE, "%s-TransactionLogTransactionDeletionRule",
            "Triggers deletion of old transactions from the active transaction logs of online Sleeper tables");

    private final InstanceProperty property;
    private final String nameFormat;
    private final String description;

    private static SleeperScheduleRule add(InstanceProperty property, String nameFormat, String description) {
        SleeperScheduleRule rule = new SleeperScheduleRule(property, nameFormat, description);
        RULES.add(rule);
        return rule;
    }

    private SleeperScheduleRule(InstanceProperty property, String nameFormat, String description) {
        this.property = requireNonNull(property, "property must not be null");
        this.nameFormat = requireNonNull(nameFormat, "nameFormat must not be null");
        this.description = requireNonNull(description, "description must not be null");
    }

    /**
     * Retrieves the list of all rule declarations.
     *
     * @return the rules
     */
    public static List<SleeperScheduleRule> all() {
        return Collections.unmodifiableList(RULES);
    }

    /**
     * Streams through all CloudWatch rules for a given Sleeper instance.
     *
     * @param  properties the instance properties
     * @return            values for each CloudWatch rule property, containing rule names
     */
    public static Stream<InstanceRule> getDeployedRules(InstanceProperties properties) {
        return RULES.stream()
                .map(rule -> rule.readValue(properties))
                .filter(InstanceRule::isDeployed);
    }

    /**
     * Streams through all CloudWatch rule default values for a given Sleeper instance.
     *
     * @param  instanceId the instance ID
     * @return            default values for each CloudWatch rule property, with rule names derived from the instance ID
     */
    public static Stream<InstanceRule> getDefaultRules(String instanceId) {
        return RULES.stream()
                .map(rule -> rule.getDefault(instanceId));
    }

    /**
     * Reads the value of this CloudWatch rule property.
     *
     * @param  properties the instance properties
     * @return            the value, containing rule names
     */
    public InstanceRule readValue(InstanceProperties properties) {
        return new InstanceRule(properties.get(property));
    }

    /**
     * Derives the default CloudWatch rule name from the instance ID, for this property.
     *
     * @param  properties the instance properties
     * @return            the default rule name
     */
    public String buildRuleName(InstanceProperties properties) {
        return buildRuleName(properties.get(ID));
    }

    /**
     * Derives the default CloudWatch rule name from the instance ID, for this property.
     *
     * @param  instanceId the instance ID
     * @return            the default value, containing rule names
     */
    public InstanceRule getDefault(String instanceId) {
        return new InstanceRule(buildRuleName(instanceId));
    }

    /**
     * Derives the default CloudWatch rule name from the instance ID, for this property.
     *
     * @param  instanceId the instance ID
     * @return            the default rule name
     */
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

    public String getDescription() {
        return description;
    }

    /**
     * A wrapper for the instance property value for this CloudWatch rule.
     */
    public class InstanceRule {
        private final String ruleName;

        private InstanceRule(String ruleName) {
            this.ruleName = ruleName;
        }

        public boolean isDeployed() {
            return ruleName != null && !ruleName.isBlank();
        }

        public String getRuleName() {
            return ruleName;
        }

        public InstanceProperty getProperty() {
            return property;
        }

        public SleeperScheduleRule getRule() {
            return SleeperScheduleRule.this;
        }
    }

}
