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
import sleeper.systemtest.dsl.statestore.StateStoreCommitterDriver;
import sleeper.systemtest.dsl.statestore.StateStoreCommitterLogs;
import sleeper.systemtest.dsl.statestore.StateStoreCommitterLogsDriver;

import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;

public class InMemoryStateStoreCommitter {

    private final InMemoryIngestByQueue ingest;
    private final InMemoryCompaction compaction;
    private final Queue<StateStoreCommitMessage> queue = new LinkedList<>();
    private final Map<String, Integer> numCommitsByTableId = new HashMap<>();
    private final Map<String, Double> commitsPerSecondByTableId = new HashMap<>();
    private final Map<String, Boolean> runCommitterOnSendByTableId = new HashMap<>();

    public InMemoryStateStoreCommitter(InMemoryIngestByQueue ingest, InMemoryCompaction compaction) {
        this.ingest = ingest;
        this.compaction = compaction;
    }

    public StateStoreCommitterDriver withContext(SystemTestContext context) {
        return new Driver(context);
    }

    public StateStoreCommitterLogsDriver logsDriver() {
        return (startTime, endTime) -> new FakeLogs(numCommitsByTableId, commitsPerSecondByTableId);
    }

    public static StateStoreCommitterLogs fakeLogsFromNumCommitsByTableId(Map<String, Integer> numCommitsByTableId) {
        return new FakeLogs(numCommitsByTableId, Map.of());
    }

    public void setRunCommitterOnSend(SleeperSystemTest sleeper, boolean runCommitterOnSend) {
        runCommitterOnSendByTableId.put(sleeper.tableProperties().get(TABLE_ID), runCommitterOnSend);
    }

    public void addFakeCommits(SleeperSystemTest sleeper, int commits) {
        numCommitsByTableId.compute(
                sleeper.tableProperties().get(TABLE_ID),
                (id, count) -> count == null ? commits : count + commits);
    }

    public void setFakeCommitsPerSecond(SleeperSystemTest sleeper, double commitsPerSecond) {
        commitsPerSecondByTableId.put(sleeper.tableProperties().get(TABLE_ID), commitsPerSecond);
    }

    public class Driver implements StateStoreCommitterDriver {
        private final SystemTestInstanceContext instance;
        private final StateStoreCommitter committer;
        private boolean committerPaused = false;

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
            if (!committerPaused && isRunCommitterOnSend()) {
                runCommitter();
            }
        }

        private void runCommitter() {
            for (StateStoreCommitMessage message = queue.poll(); message != null; message = queue.poll()) {
                try {
                    StateStoreCommitRequest appliedRequest = committer.applyFromJson(message.getBody());
                    numCommitsByTableId.compute(
                            appliedRequest.getTableId(),
                            (id, count) -> count == null ? 1 : count + 1);
                } catch (StateStoreException e) {
                    throw new RuntimeException(e);
                } catch (TableNotFoundException e) {
                    // Discard messages from other tests
                }
            }
        }

        private boolean isRunCommitterOnSend() {
            return runCommitterOnSendByTableId.getOrDefault(
                    instance.getTableStatus().getTableUniqueId(),
                    true);
        }

        @Override
        public void pauseReceivingMessages() {
            committerPaused = true;
        }

        @Override
        public void resumeReceivingMessages() {
            committerPaused = false;
            runCommitter();
        }
    }

    private static class FakeLogs implements StateStoreCommitterLogs {

        private final Map<String, Integer> numCommitsByTableId;
        private final Map<String, Double> commitsPerSecondByTableId;

        FakeLogs(Map<String, Integer> numCommitsByTableId, Map<String, Double> commitsPerSecondByTableId) {
            this.numCommitsByTableId = numCommitsByTableId;
            this.commitsPerSecondByTableId = commitsPerSecondByTableId;
        }

        @Override
        public Map<String, Integer> countNumCommitsByTableId(Set<String> tableIds) {
            Map<String, Integer> filtered = new HashMap<>();
            tableIds.forEach(tableId -> {
                Integer count = numCommitsByTableId.get(tableId);
                if (count != null) {
                    filtered.put(tableId, count);
                }
            });
            return filtered;
        }

        @Override
        public Map<String, Double> computeOverallCommitsPerSecondByTableId(Set<String> tableIds) {
            return tableIds.stream()
                    .collect(toMap(id -> id, id -> commitsPerSecondByTableId.getOrDefault(id, 1.0)));
        }
    }

    private static String failToLoadS3Object(String key) {
        throw new UnsupportedOperationException();
    }

}
