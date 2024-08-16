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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Stream;

public class SystemTestStateStoreFakeCommits {

    private final SystemTestInstanceContext instance;
    private final StateStoreCommitterDriver driver;
    private final WaitForStateStoreCommitLogs waiter;
    private final Map<String, Integer> waitForNumCommitsByTableId = new ConcurrentHashMap<>();
    private final Instant getRunsAfterTime;

    public SystemTestStateStoreFakeCommits(
            SystemTestContext context,
            StateStoreCommitterDriver driver,
            StateStoreCommitterLogsDriver logsDriver) {
        this.driver = driver;
        instance = context.instance();
        waiter = new WaitForStateStoreCommitLogs(logsDriver);
        getRunsAfterTime = context.reporting().getRecordingStartTime();
    }

    public SystemTestStateStoreFakeCommits sendBatched(Function<StateStoreCommitMessageFactory, Stream<StateStoreCommitMessage>> buildCommits) {
        send(buildCommits.apply(messageFactory()));
        return this;
    }

    public SystemTestStateStoreFakeCommits send(Function<StateStoreCommitMessageFactory, StateStoreCommitMessage> buildCommit) {
        send(Stream.of(buildCommit.apply(messageFactory())));
        return this;
    }

    public SystemTestStateStoreFakeCommits waitForCommitLogs(PollWithRetries poll) throws InterruptedException {
        waiter.waitForCommitLogs(poll, waitForNumCommitsByTableId, getRunsAfterTime);
        waitForNumCommitsByTableId.clear();
        return this;
    }

    public SystemTestStateStoreFakeCommits pauseReceivingCommitMessages() {
        driver.pauseReceivingMessages();
        return this;
    }

    public SystemTestStateStoreFakeCommits resumeReceivingCommitMessages() {
        driver.resumeReceivingMessages();
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
