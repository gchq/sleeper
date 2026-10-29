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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.util.ThreadSleep;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Checks the progress of a running compaction on a background thread. If the compaction is detected as stalled, a
 * handler is called once and the checks stop. The thread running the compaction is not interacted with, as it may be
 * blocked in native code.
 */
public class CompactionWatchdog implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompactionWatchdog.class);

    private final String jobId;
    private final Instant jobStartTime;
    private final CompactionProgressMonitor monitor;
    private final CompactionStallDetector detector;
    private final Duration checkPeriod;
    private final Supplier<Instant> timeSupplier;
    private final ThreadSleep threadSleep;
    private final Consumer<CompactionStall> stallHandler;
    private final Thread thread;
    private final Object lock = new Object();
    private boolean stopped = false;
    private boolean handlingStall = false;

    @SuppressWarnings("checkstyle:ParameterNumberCheck")
    public CompactionWatchdog(
            String jobId, Instant jobStartTime, CompactionProgressMonitor monitor, CompactionStallDetector detector,
            Duration checkPeriod, Supplier<Instant> timeSupplier, ThreadSleep threadSleep,
            Consumer<CompactionStall> stallHandler) {
        this.jobId = jobId;
        this.jobStartTime = jobStartTime;
        this.monitor = monitor;
        this.detector = detector;
        this.checkPeriod = checkPeriod;
        this.timeSupplier = timeSupplier;
        this.threadSleep = threadSleep;
        this.stallHandler = stallHandler;
        this.thread = new Thread(this::run, "compaction-watchdog-" + jobId);
        this.thread.setDaemon(true);
    }

    /**
     * Starts checking progress on a background thread.
     */
    public void start() {
        thread.start();
    }

    /**
     * Stops checking progress, and waits for the background thread to finish. If a stall has already been detected,
     * this will wait for the stall handler to finish, and will not interrupt it.
     */
    @Override
    public void close() {
        synchronized (lock) {
            stopped = true;
            if (!handlingStall) {
                thread.interrupt();
            }
        }
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void run() {
        while (true) {
            try {
                threadSleep.waitForMillis(checkPeriod.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            Optional<CompactionStall> stall;
            synchronized (lock) {
                if (stopped) {
                    return;
                }
                stall = detector.checkForStall(jobStartTime, monitor.getProgress(), timeSupplier.get());
                if (stall.isEmpty()) {
                    continue;
                }
                handlingStall = true;
            }
            LOGGER.error("Compaction job {}: detected stalled compaction: {}", jobId, stall.get().description());
            try {
                stallHandler.accept(stall.get());
            } catch (RuntimeException e) {
                LOGGER.error("Compaction job {}: failed handling stalled compaction", jobId, e);
            }
            return;
        }
    }
}
