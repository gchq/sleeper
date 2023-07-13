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
package sleeper.configuration.properties;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static sleeper.configuration.properties.CommonProperty.ID;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_JOB_CREATION_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_TASK_CREATION_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.GARBAGE_COLLECTOR_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_BATCHER_JOB_CREATION_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.PARTITION_SPLITTING_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_TASK_CREATION_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.TABLE_METRICS_RULES;


public class SleeperScheduleRule {

    private static final List<SleeperScheduleRule> RULES = new ArrayList<>();
    // Rule that creates compaction jobs
    public static final SleeperScheduleRule COMPACTION_JOB_CREATION = add(
            COMPACTION_JOB_CREATION_CLOUDWATCH_RULE, "%s-CompactionJobCreationRule");
    // Rules that create compaction and splitting compaction tasks
    public static final SleeperScheduleRule COMPACTION_TASK_CREATION = add(
            COMPACTION_TASK_CREATION_CLOUDWATCH_RULE, "%s-CompactionTasksCreationRule");
    public static final SleeperScheduleRule SPLITTING_COMPACTION_TASK_CREATION = add(
            SPLITTING_COMPACTION_TASK_CREATION_CLOUDWATCH_RULE, "%s-SplittingCompactionTasksCreationRule");
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
    public static final SleeperScheduleRule TABLE_METRICS = add(TABLE_METRICS_RULES, null);

    private final InstanceProperty property;
    private final String nameFormat;

    private static SleeperScheduleRule add(InstanceProperty property, String nameFormat) {
        SleeperScheduleRule rule = new SleeperScheduleRule(property, nameFormat);
        RULES.add(rule);
        return rule;
    }

    private SleeperScheduleRule(InstanceProperty property, String nameFormat) {
        this.property = property;
        this.nameFormat = nameFormat;
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
        if (nameFormat == null) {
            return new Value(null);
        } else {
            return new Value(buildRuleName(instanceId));
        }
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
