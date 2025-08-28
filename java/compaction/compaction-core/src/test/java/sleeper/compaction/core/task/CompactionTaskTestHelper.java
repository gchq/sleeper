/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.compaction.core.task;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobCommitterOrSendToLambda;
import sleeper.compaction.core.job.CompactionRunner;
import sleeper.compaction.core.task.CompactionTask.MessageReceiver;
import sleeper.core.properties.PropertiesReloader;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.task.CompactionTaskTracker;
import sleeper.core.util.ThreadSleep;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static sleeper.core.testutils.SupplierTestHelper.supplyNumberedUuidsWithPrefix;
import static sleeper.core.testutils.SupplierTestHelper.timePassesAMinuteAtATimeFrom;
import static sleeper.core.util.ThreadSleepTestHelper.noWaits;

public class CompactionTaskTestHelper {

    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final CompactionJobTracker jobTracker;
    private final Supplier<Instant> timeSupplier;
    private final ThreadSleep threadSleep;

    public CompactionTaskTestHelper(
            InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider, CompactionJobTracker jobTracker) {
        this(instanceProperties, tablePropertiesProvider, stateStoreProvider, jobTracker,
                timePassesAMinuteAtATimeFrom(Instant.parse("2025-08-28T12:00:00Z")),
                noWaits());
    }

    public CompactionTaskTestHelper(
            InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider, CompactionJobTracker jobTracker,
            Supplier<Instant> timeSupplier, ThreadSleep threadSleep) {
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.jobTracker = jobTracker;
        this.timeSupplier = timeSupplier;
        this.threadSleep = threadSleep;
    }

    public static MessageReceiver receiveJobs(List<CompactionJob> jobs) {
        LinkedList<CompactionJob> queue = new LinkedList<>(jobs);
        return () -> {
            if (queue.isEmpty()) {
                return Optional.empty();
            } else {
                return Optional.of(FakeMessageHandle.fromQueue(queue));
            }
        };
    }

    public void runTask(CompactionRunner compactionRunner, List<CompactionJob> jobs) throws IOException {
        runTask(receiveJobs(jobs), compactionRunner);
    }

    public void runTask(MessageReceiver messageReceiver, CompactionRunner compactionRunner) throws IOException {
        new CompactionTask(
                instanceProperties, tablePropertiesProvider, PropertiesReloader.neverReload(),
                stateStoreProvider, messageReceiver, stateStoreWaitForFiles(), jobCommitter(),
                jobTracker, CompactionTaskTracker.NONE,
                runnerFactory(compactionRunner), "test-task", supplyNumberedUuidsWithPrefix("testrun"), timeSupplier, threadSleep)
                .run();
    }

    private StateStoreWaitForFiles stateStoreWaitForFiles() {
        return new StateStoreWaitForFilesTestHelper(
                tablePropertiesProvider, stateStoreProvider,
                jobTracker, threadSleep, timeSupplier)
                .withAttempts(1);
    }

    private CompactionJobCommitterOrSendToLambda jobCommitter() {
        return new CompactionJobCommitterOrSendToLambda(
                tablePropertiesProvider, stateStoreProvider, jobTracker,
                request -> {
                    throw new IllegalStateException("Found unexpected async commit request: " + request);
                },
                batchedRequest -> {
                    throw new IllegalStateException("Found unexpected batched async commit request: " + batchedRequest);
                }, timeSupplier);
    }

    private CompactionRunnerFactory runnerFactory(CompactionRunner compactionRunner) {
        return (job, table) -> compactionRunner;
    }

}
