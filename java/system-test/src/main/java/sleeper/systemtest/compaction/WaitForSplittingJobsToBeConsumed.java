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

package sleeper.systemtest.compaction;

import sleeper.clients.util.PollWithRetries;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.job.common.QueueMessageCount;
import sleeper.systemtest.util.WaitForQueueEstimate;

import static java.util.function.Predicate.not;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_JOB_QUEUE_URL;

public class WaitForSplittingJobsToBeConsumed {

    private WaitForSplittingJobsToBeConsumed() {
    }

    public static WaitForQueueEstimate from(
            QueueMessageCount.Client client, InstanceProperties properties, String tableName,
            CompactionJobStatusStore statusStore, PollWithRetries poll) {

        return WaitForQueueEstimate.withCustomPredicate(
                client, properties, SPLITTING_COMPACTION_JOB_QUEUE_URL, "all jobs consumed",
                count -> count.getApproximateNumberOfMessages() > 0
                        || statusStore.getAllJobs(tableName).stream().anyMatch(not(CompactionJobStatus::isStarted)),
                poll);
    }
}
