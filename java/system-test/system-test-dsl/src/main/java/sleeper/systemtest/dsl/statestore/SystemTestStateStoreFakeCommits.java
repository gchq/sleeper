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

import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summingInt;

public class SystemTestStateStoreFakeCommits {

    private final SystemTestInstanceContext instance;
    private final StateStoreCommitterDriver driver;
    private final Map<String, Integer> waitForNumCommitsByTableId = new ConcurrentHashMap<>();
    private Instant findCommitsFromTime;

    public SystemTestStateStoreFakeCommits(SystemTestContext context) {
        instance = context.instance();
        driver = context.instance().adminDrivers().stateStoreCommitter(context);
        findCommitsFromTime = context.reporting().getRecordingStartTime();
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
        poll.pollUntil("all state store commits are applied", () -> {
            List<StateStoreCommitterRun> runs = driver.getRunsAfter(findCommitsFromTime);
            decrementWaitForNumCommits(runs);
            updateFindCommitsFromTime(runs);
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

    private void decrementWaitForNumCommits(List<StateStoreCommitterRun> runs) {
        Map<String, Integer> numCommitsByTableId = runs.stream()
                .flatMap(run -> run.getCommits().stream())
                .collect(groupingBy(StateStoreCommitSummary::getTableId, summingInt(commit -> 1)));
        numCommitsByTableId.forEach((tableId, numCommits) -> {
            waitForNumCommitsByTableId.compute(tableId, (id, count) -> {
                if (count == null) {
                    return null;
                } else if (numCommits >= count) {
                    return null;
                } else {
                    return count - numCommits;
                }
            });
        });
    }

    private void updateFindCommitsFromTime(List<StateStoreCommitterRun> runs) {
        runs.stream()
                .map(StateStoreCommitterRun::getLastTime)
                .max(Comparator.naturalOrder())
                .ifPresent(lastTime -> findCommitsFromTime = lastTime);
    }

    private StateStoreCommitMessageFactory messageFactory() {
        return new StateStoreCommitMessageFactory(instance.getTableStatus().getTableUniqueId());
    }
}
