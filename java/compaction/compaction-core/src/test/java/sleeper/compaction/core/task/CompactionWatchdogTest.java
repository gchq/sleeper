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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import sleeper.core.testutils.TestInstantSupplier;
import sleeper.core.util.ThreadSleep;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.testutils.SupplierTestHelper.supplyTimes;

public class CompactionWatchdogTest {

    private static final Instant JOB_START = Instant.parse("2026-10-26T10:00:00Z");
    private static final Duration CHECK_PERIOD = Duration.ofSeconds(30);

    private final CompactionStallDetector detector = new CompactionStallDetector(
            Duration.ofMinutes(10), Duration.ofMinutes(5));
    private final ControlledSleep sleep = new ControlledSleep();
    private final List<CompactionStall> stalls = new CopyOnWriteArrayList<>();
    private final CountDownLatch stallHandled = new CountDownLatch(1);
    private CompactionWatchdog watchdog;

    @AfterEach
    void tearDown() {
        if (watchdog != null) {
            watchdog.close();
        }
    }

    @Test
    void shouldCallHandlerOnceWhenStalled() throws Exception {
        // Given
        TestInstantSupplier checkTimes = supplyTimes(
                Instant.parse("2026-10-26T10:05:00Z"),
                Instant.parse("2026-10-26T10:10:00Z"));
        startWatchdog(new CompactionProgressMonitor(supplyTimes()), checkTimes);

        // When
        sleep.awaitWaitThenTick();
        sleep.awaitWaitThenTick();

        // Then
        assertThat(stallHandled.await(10, TimeUnit.SECONDS)).isTrue();
        watchdog.close();
        assertThat(stalls).containsExactly(new CompactionStall(
                "No progress update received in 10m 0s since compaction started, exceeding maximum of 10m 0s",
                Instant.parse("2026-10-26T10:10:00Z")));
        assertThat(checkTimes.getRemainingTimes()).isEmpty();
        assertThat(sleep.waits).containsExactly(CHECK_PERIOD, CHECK_PERIOD);
    }

    @Test
    void shouldNotCallHandlerWhileProgressing() throws Exception {
        // Given
        CompactionProgressMonitor monitor = new CompactionProgressMonitor(supplyTimes(
                Instant.parse("2026-10-26T10:04:00Z"),
                Instant.parse("2026-10-26T10:08:00Z")));
        TestInstantSupplier checkTimes = supplyTimes(
                Instant.parse("2026-10-26T10:05:00Z"),
                Instant.parse("2026-10-26T10:12:00Z"));
        startWatchdog(monitor, checkTimes);

        // When
        monitor.accept(100);
        sleep.awaitWaitThenTick();
        monitor.accept(200);
        sleep.awaitWaitThenTick();
        sleep.awaitWait();
        watchdog.close();

        // Then
        assertThat(stalls).isEmpty();
        assertThat(checkTimes.getRemainingTimes()).isEmpty();
    }

    @Test
    void shouldStopWithoutCheckingWhenClosed() throws Exception {
        // Given
        TestInstantSupplier checkTimes = supplyTimes();
        startWatchdog(new CompactionProgressMonitor(supplyTimes()), checkTimes);
        sleep.awaitWait();

        // When
        watchdog.close();

        // Then
        assertThat(stalls).isEmpty();
        assertThat(sleep.waits).containsExactly(CHECK_PERIOD);
    }

    private void startWatchdog(CompactionProgressMonitor monitor, TestInstantSupplier checkTimes) {
        watchdog = new CompactionWatchdog("test-job", JOB_START, monitor, detector, CHECK_PERIOD,
                checkTimes, sleep, stall -> {
                    stalls.add(stall);
                    stallHandled.countDown();
                });
        watchdog.start();
    }

    /**
     * A fake sleep which blocks the watchdog thread until the test allows it to continue.
     */
    private static class ControlledSleep implements ThreadSleep {
        private final List<Duration> waits = new CopyOnWriteArrayList<>();
        private final BlockingQueue<Object> waitsStarted = new LinkedBlockingQueue<>();
        private final BlockingQueue<Object> ticks = new LinkedBlockingQueue<>();

        @Override
        public void waitForMillis(long milliseconds) throws InterruptedException {
            waits.add(Duration.ofMillis(milliseconds));
            waitsStarted.put(new Object());
            ticks.take();
        }

        void awaitWait() throws InterruptedException {
            assertThat(waitsStarted.poll(10, TimeUnit.SECONDS))
                    .as("watchdog should wait")
                    .isNotNull();
        }

        void awaitWaitThenTick() throws InterruptedException {
            awaitWait();
            ticks.put(new Object());
        }
    }
}
