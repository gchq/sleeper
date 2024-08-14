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
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Stream;

public class SystemTestStateStoreFakeCommits {
    public static final Logger LOGGER = LoggerFactory.getLogger(SystemTestStateStoreFakeCommits.class);

    private static final Duration QUERY_RUNS_TIME_SLACK = Duration.ofSeconds(5);

    private final SystemTestInstanceContext instance;
    private final StateStoreCommitterDriver driver;
    private final Map<String, Integer> waitForNumCommitsByTableId = new ConcurrentHashMap<>();
    private Instant getRunsAfterTime;

    public SystemTestStateStoreFakeCommits(SystemTestContext context) {
        instance = context.instance();
        driver = context.instance().adminDrivers().stateStoreCommitter(context);
        getRunsAfterTime = context.reporting().getRecordingStartTime().minus(QUERY_RUNS_TIME_SLACK);
    }

    public SystemTestStateStoreFakeCommits sendBatched(Function<StateStoreCommitMessageFactory, Stream<StateStoreCommitMessage>> buildCommits) {
        send(buildCommits.apply(messageFactory()));
        return this;
    }

    public SystemTestStateStoreFakeCommits send(Function<StateStoreCommitMessageFactory, StateStoreCommitMessage> buildCommit) {
        send(Stream.of(buildCommit.apply(messageFactory())));
        return this;
    }

    public SystemTestStateStoreFakeCommits waitForCommits(PollWithRetries poll) throws InterruptedException {
        LOGGER.info("Waiting for commits by table ID: {}", waitForNumCommitsByTableId);
        poll.pollUntil("all state store commits are applied", () -> {
            List<StateStoreCommitterLogEntry> logs = driver.getLogsInPeriod(getRunsAfterTime, Instant.now().plus(QUERY_RUNS_TIME_SLACK));
            WaitForStateStoreCommits.decrementWaitForNumCommits(logs, waitForNumCommitsByTableId);
            logs.stream()
                    .map(StateStoreCommitterLogEntry::getTimeInCommitter)
                    .max(Comparator.naturalOrder())
                    .ifPresent(lastTime -> getRunsAfterTime = lastTime.minus(QUERY_RUNS_TIME_SLACK));
            LOGGER.info("Remaining unapplied commits by table ID: {}", waitForNumCommitsByTableId);
            return waitForNumCommitsByTableId.isEmpty();
        });
        return this;
    }

    private void send(Stream<StateStoreCommitMessage> messages) {
        driver.sendCommitMessages(messages
                .peek(message -> waitForNumCommitsByTableId.compute(
                        message.getTableId(),
                        (id, count) -> count == null ? 1 : count + 1)));
    }

    private StateStoreCommitMessageFactory messageFactory() {
        return new StateStoreCommitMessageFactory(instance.getTableStatus().getTableUniqueId());
    }
}
