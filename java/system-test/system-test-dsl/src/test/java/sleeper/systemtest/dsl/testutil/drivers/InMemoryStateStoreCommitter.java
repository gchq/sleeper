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
package sleeper.systemtest.dsl.testutil.drivers;

import sleeper.commit.StateStoreCommitRequest;
import sleeper.commit.StateStoreCommitter;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.table.TableNotFoundException;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.statestore.StateStoreCommitMessage;
import sleeper.systemtest.dsl.statestore.StateStoreCommitSummary;
import sleeper.systemtest.dsl.statestore.StateStoreCommitterDriver;
import sleeper.systemtest.dsl.statestore.StateStoreCommitterLogEntry;
import sleeper.systemtest.dsl.statestore.StateStoreCommitterLogsDriver;
import sleeper.systemtest.dsl.statestore.StateStoreCommitterRunFinished;
import sleeper.systemtest.dsl.statestore.StateStoreCommitterRunStarted;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;

public class InMemoryStateStoreCommitter {

    private final InMemoryIngestByQueue ingest;
    private final InMemoryCompaction compaction;
    private final Queue<StateStoreCommitMessage> queue = new LinkedList<>();
    private final List<StateStoreCommitterLogEntry> logs = new ArrayList<>();
    private final Map<String, Boolean> runCommitterOnSendByTableId = new HashMap<>();

    public InMemoryStateStoreCommitter(InMemoryIngestByQueue ingest, InMemoryCompaction compaction) {
        this.ingest = ingest;
        this.compaction = compaction;
    }

    public StateStoreCommitterDriver withContext(SystemTestContext context) {
        return new Driver(context);
    }

    public StateStoreCommitterLogsDriver logsDriver() {
        return logsDriver(logs);
    }

    public void setRunCommitterOnSend(SleeperSystemTest sleeper, boolean runCommitterOnSend) {
        runCommitterOnSendByTableId.put(sleeper.tableProperties().get(TABLE_ID), runCommitterOnSend);
    }

    public void addFakeLog(StateStoreCommitterLogEntry entry) {
        logs.add(entry);
    }

    public static StateStoreCommitterLogsDriver logsDriver(List<StateStoreCommitterLogEntry> logs) {
        return (startTime, endTime) -> logs.stream()
                .filter(run -> run.getTimeInCommitter().compareTo(startTime) >= 0)
                .filter(run -> run.getTimeInCommitter().compareTo(endTime) <= 0)
                .collect(toUnmodifiableList());
    }

    public class Driver implements StateStoreCommitterDriver {
        private final SystemTestInstanceContext instance;
        private final StateStoreCommitter committer;

        private Driver(SystemTestContext context) {
            instance = context.instance();
            TablePropertiesProvider tablePropertiesProvider = instance.getTablePropertiesProvider();
            committer = new StateStoreCommitter(tablePropertiesProvider,
                    compaction.jobStore(), ingest.jobStore(),
                    instance.getStateStoreProvider().byTableId(tablePropertiesProvider),
                    InMemoryStateStoreCommitter::failToLoadS3Object, Instant::now);
        }

        @Override
        public void sendCommitMessages(Stream<StateStoreCommitMessage> messages) {
            messages.forEach(queue::add);
            if (isRunCommitterOnSend()) {
                runCommitter();
            }
        }

        private void runCommitter() {
            Instant startTime = Instant.now();
            List<StateStoreCommitSummary> commits = new ArrayList<>();
            for (StateStoreCommitMessage message = queue.poll(); message != null; message = queue.poll()) {
                try {
                    StateStoreCommitRequest appliedRequest = committer.applyFromJson(message.getBody());
                    commits.add(new StateStoreCommitSummary(
                            "test-stream", appliedRequest.getTableId(), appliedRequest.getRequest().getClass().getSimpleName(), Instant.now()));
                } catch (StateStoreException e) {
                    throw new RuntimeException(e);
                } catch (TableNotFoundException e) {
                    // Discard messages from other tests
                }
            }
            if (!commits.isEmpty()) {
                logs.add(new StateStoreCommitterRunStarted("test-stream", startTime));
                logs.addAll(commits);
                logs.add(new StateStoreCommitterRunFinished("test-stream", Instant.now()));
            }
        }

        private boolean isRunCommitterOnSend() {
            return runCommitterOnSendByTableId.getOrDefault(
                    instance.getTableStatus().getTableUniqueId(),
                    true);
        }
    }

    private static String failToLoadS3Object(String key) {
        throw new UnsupportedOperationException();
    }

}
