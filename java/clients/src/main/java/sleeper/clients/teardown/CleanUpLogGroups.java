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
package sleeper.clients.teardown;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogGroup;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

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
        run(logs, cloudFormation, Instant.now(), () ->
                // See https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/cloudwatch_limits_cwl.html
                sleepForSustainedRatePerSecond(4));
    }

    public static void run(CloudWatchLogsClient logsClient, CloudFormationClient cloudFormation, Instant queryTime, Runnable sleepForRateLimit) {
        planDeletions(logsClient, cloudFormation, queryTime)
                .delete(logsClient, sleepForRateLimit);
    }

    private static DeletionPlan planDeletions(
            CloudWatchLogsClient logsClient, CloudFormationClient cloudFormation, Instant queryTime) {
        Instant maxCreationTime = queryTime.minus(Duration.ofDays(30));
        CloudFormationStacks stacks = new CloudFormationStacks(cloudFormation);
        DeletionPlan plan = new DeletionPlan(stacks, maxCreationTime);
        logsClient.describeLogGroupsPaginator().logGroups().forEach(plan::add);
        return plan;
    }

    public static class DeletionPlan {
        private final List<String> delete = new ArrayList<>();
        private int numEmpty;
        private int numOld;
        private int numToKeep;
        private final CloudFormationStacks stacks;
        private final Instant maxCreationTime;

        public DeletionPlan(CloudFormationStacks stacks, Instant maxCreationTime) {
            this.stacks = stacks;
            this.maxCreationTime = maxCreationTime;
        }

        public void add(LogGroup logGroup) {
            if (isDelete(logGroup)) {
                delete.add(logGroup.logGroupName());
            } else {
                numToKeep++;
            }
        }

        private boolean isDelete(LogGroup logGroup) {
            if (stacks.anyIn(logGroup.logGroupName())) {
                return false;
            } else if (logGroup.storedBytes() == 0) {
                numEmpty++;
                return true;
            } else if (isOld(logGroup)) {
                numOld++;
                return true;
            } else {
                return false;
            }
        }

        private boolean isOld(LogGroup logGroup) {
            return logGroup.retentionInDays() == null &&
                    Instant.ofEpochMilli(logGroup.creationTime()).isBefore(maxCreationTime);
        }

        public void delete(CloudWatchLogsClient logsClient, Runnable sleepForRateLimit) {
            logTotals();

            int numToDelete = delete.size();
            for (int i = 0; i < numToDelete; i++) {
                if (i % 50 == 0) {
                    LOGGER.info("Deleting, done {} of {}", i, numToDelete);
                }
                String logGroupName = delete.get(i);

                sleepForRateLimit.run();
                logsClient.deleteLogGroup(builder -> builder.logGroupName(logGroupName));
            }

            LOGGER.info("Finished deleting {} empty/old log groups not in stacks", numToDelete);
        }

        private void logTotals() {
            LOGGER.info("Keeping all groups containing stack names: {}", stacks.getStackNames());
            LOGGER.info("Groups to delete: {}", delete.size());
            LOGGER.info("Empty groups to delete: {}", numEmpty);
            LOGGER.info("Old groups to delete: {}", numOld);
            LOGGER.info("Groups to keep: {}", numToKeep);
        }
    }
}
