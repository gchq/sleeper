/*
 * Copyright 2022-2026 Crown Copyright
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

import sleeper.core.properties.model.CompactionProgressCheckSettings;
import sleeper.core.util.ThreadSleep;

import java.time.Instant;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Creates watchdogs to check the progress of compactions, and terminates the compaction task if a compaction stalls.
 */
public class CompactionWatchdogFactory {

    private final CompactionTaskTerminator terminator;
    private final Supplier<Instant> timeSupplier;
    private final ThreadSleep threadSleep;

    private CompactionWatchdogFactory(CompactionTaskTerminator terminator, Supplier<Instant> timeSupplier, ThreadSleep threadSleep) {
        this.terminator = Objects.requireNonNull(terminator, "terminator must not be null");
        this.timeSupplier = Objects.requireNonNull(timeSupplier, "timeSupplier must not be null");
        this.threadSleep = Objects.requireNonNull(threadSleep, "threadSleep must not be null");
    }

    /**
     * Creates a watchdog factory that uses the system clock.
     *
     * @param  terminator the terminator to call when a compaction stalls
     * @return            the factory
     */
    public static CompactionWatchdogFactory terminateOnStall(CompactionTaskTerminator terminator) {
        return new CompactionWatchdogFactory(terminator, Instant::now, Thread::sleep);
    }

    /**
     * Creates a watchdog factory with a specified clock and wait between progress checks. Used in tests.
     *
     * @param  terminator        the terminator to call when a compaction stalls
     * @param  timeSupplier      the clock to use to time progress updates and checks
     * @param  waitBetweenChecks the wait between progress checks
     * @return                   the factory
     */
    public static CompactionWatchdogFactory terminateOnStall(
            CompactionTaskTerminator terminator, Supplier<Instant> timeSupplier, ThreadSleep waitBetweenChecks) {
        return new CompactionWatchdogFactory(terminator, timeSupplier, waitBetweenChecks);
    }

    /**
     * Creates a progress monitor to receive progress updates from a compaction.
     *
     * @return the monitor
     */
    public CompactionProgressMonitor createMonitor() {
        return new CompactionProgressMonitor(timeSupplier);
    }

    /**
     * Creates a watchdog to check the progress of a compaction. When a stall is detected, the stall handler is called,
     * and then the compaction task is terminated. The task is terminated even if the stall handler fails.
     *
     * @param  jobId        the ID of the compaction job
     * @param  monitor      the monitor receiving progress updates from the compaction
     * @param  settings     the settings for checking progress
     * @param  stallHandler a handler to report the stall before the task is terminated
     * @return              the watchdog, which must be started
     */
    public CompactionWatchdog createWatchdog(
            String jobId, CompactionProgressMonitor monitor, CompactionProgressCheckSettings settings,
            Consumer<CompactionStall> stallHandler) {
        return new CompactionWatchdog(jobId, timeSupplier.get(), monitor,
                CompactionStallDetector.from(settings), settings.checkPeriod(), timeSupplier, threadSleep,
                stall -> {
                    try {
                        stallHandler.accept(stall);
                    } finally {
                        terminator.terminate("Compaction job " + jobId + " stalled: " + stall.description());
                    }
                });
    }
}
