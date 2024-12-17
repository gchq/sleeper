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
package sleeper.systemtest.dsl.compaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.core.job.CompactionJobStatusStore;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

public class WaitForCompactionJobCreation {
    public static final Logger LOGGER = LoggerFactory.getLogger(WaitForCompactionJobCreation.class);

    private final SystemTestInstanceContext instance;
    private final CompactionJobStatusStore store;

    public WaitForCompactionJobCreation(SystemTestInstanceContext instance, CompactionDriver driver) {
        this.instance = instance;
        this.store = driver.getJobStatusStore();
    }

    public List<String> createJobsGetIds(int expectedJobs, PollWithRetries poll, Runnable createJobs) {
        Set<String> jobsBefore = allJobIds()
                .collect(Collectors.toSet());
        createJobs.run();
        try {
            List<String> newJobs = poll.queryUntil("compaction jobs were created",
                    () -> newJobIds(jobsBefore),
                    meetsExpectedJobs(expectedJobs));
            LOGGER.info("Created {} new compaction jobs", newJobs.size());
            return newJobs;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private List<String> newJobIds(Set<String> jobsBefore) {
        List<String> jobIds = allJobIds()
                .filter(not(jobsBefore::contains))
                .collect(toUnmodifiableList());
        LOGGER.info("Found {} new compaction jobs", jobIds.size());
        return jobIds;
    }

    private static Predicate<List<String>> meetsExpectedJobs(int expectedJobs) {
        return jobIds -> {
            if (jobIds.size() > expectedJobs) {
                throw new IllegalStateException("Expected " + expectedJobs + " new compaction jobs, found " + jobIds.size());
            } else {
                return jobIds.size() == expectedJobs;
            }
        };
    }

    private Stream<String> allJobIds() {
        return instance.streamTableProperties()
                .map(properties -> properties.get(TABLE_ID))
                .parallel()
                .flatMap(tableId -> store.streamAllJobs(tableId)
                        .map(CompactionJobStatus::getJobId));
    }

}
