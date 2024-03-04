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

import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;

public class WaitForCompactionJobCreation {
    public static final Logger LOGGER = LoggerFactory.getLogger(WaitForCompactionJobCreation.class);

    private final SystemTestInstanceContext instance;
    private final CompactionDriver driver;

    public WaitForCompactionJobCreation(SystemTestInstanceContext instance, CompactionDriver driver) {
        this.instance = instance;
        this.driver = driver;
    }

    public List<String> createJobsGetIds(int expectedJobs, PollWithRetries poll) {
        Set<String> jobsBefore = allJobIds()
                .collect(Collectors.toSet());
        driver.triggerCreateJobs();
        try {
            List<String> newJobs = poll.queryUntil("jobs were created",
                    () -> allJobIds()
                            .filter(not(jobsBefore::contains))
                            .collect(toUnmodifiableList()),
                    jobs -> jobs.size() >= expectedJobs);
            LOGGER.info("Created {} new compaction jobs", newJobs.size());
            return newJobs;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private Stream<String> allJobIds() {
        return instance.streamTableProperties()
                .map(properties -> properties.get(TABLE_ID))
                .parallel()
                .flatMap(tableId -> driver.getJobStatusStore().streamAllJobs(tableId)
                        .map(CompactionJobStatus::getJobId));
    }

}
