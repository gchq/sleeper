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
package sleeper.systemtest.dsl.statestore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.util.PollWithRetries;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class WaitForStateStoreCommitLogs {
    public static final Logger LOGGER = LoggerFactory.getLogger(WaitForStateStoreCommitLogs.class);
    private static final Duration QUERY_RUNS_TIME_SLACK = Duration.ofSeconds(5);

    private final StateStoreCommitterLogsDriver driver;

    public WaitForStateStoreCommitLogs(StateStoreCommitterLogsDriver driver) {
        this.driver = driver;
    }

    public StateStoreCommitterLogs waitForCommitLogs(PollWithRetries poll, Map<String, Integer> waitForNumCommitsByTableId, Instant getRunsAfterTime) throws InterruptedException {
        LOGGER.info("Waiting for commits by table ID: {}", waitForNumCommitsByTableId);
        Instant startTime = getRunsAfterTime.minus(QUERY_RUNS_TIME_SLACK);
        return poll.queryUntil("all state store commits are applied", () -> {
            Instant endTime = Instant.now().plus(QUERY_RUNS_TIME_SLACK);
            return driver.getLogsInPeriod(startTime, endTime);
        }, logs -> getRemainingCommits(waitForNumCommitsByTableId, logs).isEmpty());
    }

    public static Map<String, Integer> getRemainingCommits(Map<String, Integer> waitForNumCommitsByTableId, StateStoreCommitterLogs logs) {
        Map<String, Integer> numCommitsByTableId = logs.getNumCommitsByTableId();
        Map<String, Integer> remainingCommitsByTableId = getRemainingCommitsByTableId(waitForNumCommitsByTableId, numCommitsByTableId);
        LOGGER.info("Remaining unapplied commits by table ID: {}", remainingCommitsByTableId);
        return remainingCommitsByTableId;
    }

    private static Map<String, Integer> getRemainingCommitsByTableId(
            Map<String, Integer> waitForNumCommitsByTableId, Map<String, Integer> numCommitsByTableId) {
        Map<String, Integer> remainingCommitsByTableId = new HashMap<>(waitForNumCommitsByTableId);
        numCommitsByTableId.forEach((tableId, numCommits) -> {
            remainingCommitsByTableId.compute(tableId, (id, count) -> {
                if (count == null) {
                    return null;
                } else if (numCommits >= count) {
                    return null;
                } else {
                    return count - numCommits;
                }
            });
        });
        return remainingCommitsByTableId;
    }
}
