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
package sleeper.status.update;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.cloudformation.model.StackStatus;
import software.amazon.awssdk.services.cloudformation.model.StackSummary;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogGroup;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.core.util.RateLimitUtils.sleepForSustainedRatePerSecond;

public class CleanUpLogGroups {
    private static final Logger LOGGER = LoggerFactory.getLogger(CleanUpLogGroups.class);

    private CleanUpLogGroups() {
    }

    public static void main(String[] args) {
        try (CloudWatchLogsClient logs = CloudWatchLogsClient.create()) {
            try (CloudFormationClient cloudFormation = CloudFormationClient.create()) {
                run(logs, cloudFormation);
            }
        }
    }

    public static void run(CloudWatchLogsClient logs, CloudFormationClient cloudFormation) {
        Stacks stacks = new Stacks(cloudFormation);
        LogGroups all = new LogGroups();
        LogGroups inStacks = new LogGroups();
        LogGroups notInStacks = new LogGroups();
        for (LogGroup group : logs.describeLogGroupsPaginator().logGroups()) {
            LOGGER.info("Group {} has size {}, retention {} days", group.logGroupName(), group.storedBytes(), group.retentionInDays());
            if (stacks.anyIn(group.logGroupName())) {
                inStacks.add(group);
            } else {
                notInStacks.add(group);
            }
            all.add(group);
        }
        LOGGER.info("Compared against stack names: {}", stacks.stackNames);
        LOGGER.info("Found {} groups, {} empty, {} non-empty",
                all.count(), all.countEmpty(), all.countNotEmpty());
        LOGGER.info("Groups not containing a stack name: {}, {} empty, {} non-empty",
                notInStacks.count(), notInStacks.countEmpty(), notInStacks.countNotEmpty());
        LOGGER.info("Groups containing a stack name: {}, {} empty, {} non-empty",
                inStacks.count(), inStacks.countEmpty(), inStacks.countNotEmpty());

        int numToDelete = notInStacks.countEmpty();
        LOGGER.info("Groups to delete: {}", numToDelete);
        for (int i = 0; i < numToDelete; i++) {
            String name = notInStacks.empty.get(i);
            if (i % 50 == 0) {
                LOGGER.info("Deleting, done {} of {}", i, numToDelete);
            }

            // See https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/cloudwatch_limits_cwl.html
            sleepForSustainedRatePerSecond(4);

            logs.deleteLogGroup(builder -> builder.logGroupName(name));
        }
        LOGGER.info("Finished deleting {} empty log groups not in stacks", numToDelete);
    }

    public static void run(CloudWatchLogsClient logs, CloudFormationClient cloudFormation, Instant queryTime, Runnable sleepForRateLimit) {
        planDeletions(logs, cloudFormation, queryTime).delete(logs, sleepForRateLimit);
    }

    private static DeletionPlan planDeletions(
            CloudWatchLogsClient logs, CloudFormationClient cloudFormation, Instant queryTime) {
        Instant maxCreationTime = queryTime.minus(Duration.ofDays(30));
        Stacks stacks = new Stacks(cloudFormation);
        DeletionPlan plan = new DeletionPlan(stacks, maxCreationTime);
        logs.describeLogGroupsPaginator().logGroups().forEach(plan::add);
        return plan;
    }

    public static class Stacks {
        private final List<String> stackNames;

        public Stacks(CloudFormationClient cloudFormation) {
            stackNames = cloudFormation.listStacksPaginator(b -> b.stackStatusFilters(
                            StackStatus.CREATE_COMPLETE, StackStatus.UPDATE_COMPLETE)).stackSummaries()
                    .stream()
                    .filter(stack -> stack.parentId() == null)
                    .map(StackSummary::stackName).collect(Collectors.toList());
        }

        public boolean anyIn(String string) {
            return stackNames.stream().anyMatch(string::contains);
        }
    }

    public static class LogGroups {
        private final List<String> names = new ArrayList<>();
        private final List<String> empty = new ArrayList<>();
        private final List<String> notEmpty = new ArrayList<>();

        public void add(LogGroup logGroup) {
            names.add(logGroup.logGroupName());
            if (logGroup.storedBytes() > 0) {
                notEmpty.add(logGroup.logGroupName());
            } else {
                empty.add(logGroup.logGroupName());
            }
        }

        public int count() {
            return names.size();
        }

        public int countEmpty() {
            return empty.size();
        }

        public int countNotEmpty() {
            return notEmpty.size();
        }
    }

    public static class DeletionPlan {
        private final List<String> empty = new ArrayList<>();
        private final List<String> oldest = new ArrayList<>();
        private final List<String> toKeep = new ArrayList<>();
        private final Stacks stacks;
        private final Instant maxCreationTime;

        public DeletionPlan(Stacks stacks, Instant maxCreationTime) {
            this.stacks = stacks;
            this.maxCreationTime = maxCreationTime;
        }

        public void add(LogGroup logGroup) {
            if (stacks.anyIn(logGroup.logGroupName())) {
                toKeep.add(logGroup.logGroupName());
                return;
            }
            if (logGroup.storedBytes() == 0) {
                empty.add(logGroup.logGroupName());
            } else if (isOld(logGroup, maxCreationTime)) {
                oldest.add(logGroup.logGroupName());
            } else {
                toKeep.add(logGroup.logGroupName());
            }
        }

        public void delete(CloudWatchLogsClient logsClient, Runnable sleepForRateLimit) {
            logTotals();
            Stream.concat(empty.stream(), oldest.stream()).forEach(name -> {
                sleepForRateLimit.run();
                logsClient.deleteLogGroup(builder -> builder.logGroupName(name));
            });
        }

        private void logTotals() {
            LOGGER.info("Groups to delete: {}", empty.size() + oldest.size());
            LOGGER.info("Empty groups to delete: {}", empty.size());
            LOGGER.info("Old groups to delete: {}", oldest.size());
            LOGGER.info("Groups to keep: {}", toKeep.size());
        }

        private static boolean isOld(LogGroup logGroup, Instant maxCreationTime) {
            return logGroup.retentionInDays() == null &&
                    Instant.ofEpochMilli(logGroup.creationTime()).isBefore(maxCreationTime);
        }
    }
}
