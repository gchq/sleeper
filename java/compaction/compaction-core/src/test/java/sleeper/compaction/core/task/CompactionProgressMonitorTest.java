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

import org.junit.jupiter.api.Test;

import sleeper.core.testutils.TestInstantSupplier;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.testutils.SupplierTestHelper.supplyTimes;

public class CompactionProgressMonitorTest {

    @Test
    void shouldHaveNoProgressBeforeFirstUpdate() {
        // Given
        CompactionProgressMonitor monitor = new CompactionProgressMonitor(supplyTimes());

        // When / Then
        assertThat(monitor.getProgress()).isEmpty();
    }

    @Test
    void shouldRecordFirstUpdate() {
        // Given
        CompactionProgressMonitor monitor = new CompactionProgressMonitor(supplyTimes(
                Instant.parse("2026-10-26T10:00:05Z")));

        // When
        monitor.accept(100);

        // Then
        assertThat(monitor.getProgress()).contains(new CompactionProgress(100,
                Instant.parse("2026-10-26T10:00:05Z"),
                Instant.parse("2026-10-26T10:00:05Z")));
    }

    @Test
    void shouldMoveChangeTimeWhenRowsIncrease() {
        // Given
        CompactionProgressMonitor monitor = new CompactionProgressMonitor(supplyTimes(
                Instant.parse("2026-10-26T10:00:05Z"),
                Instant.parse("2026-10-26T10:00:10Z")));

        // When
        monitor.accept(100);
        monitor.accept(200);

        // Then
        assertThat(monitor.getProgress()).contains(new CompactionProgress(200,
                Instant.parse("2026-10-26T10:00:10Z"),
                Instant.parse("2026-10-26T10:00:10Z")));
    }

    @Test
    void shouldNotMoveChangeTimeWhenRowsUnchanged() {
        // Given
        TestInstantSupplier times = supplyTimes(
                Instant.parse("2026-10-26T10:00:05Z"),
                Instant.parse("2026-10-26T10:00:10Z"),
                Instant.parse("2026-10-26T10:00:15Z"));
        CompactionProgressMonitor monitor = new CompactionProgressMonitor(times);

        // When
        monitor.accept(100);
        monitor.accept(100);
        monitor.accept(100);

        // Then
        assertThat(monitor.getProgress()).contains(new CompactionProgress(100,
                Instant.parse("2026-10-26T10:00:15Z"),
                Instant.parse("2026-10-26T10:00:05Z")));
        assertThat(times.getRemainingTimes()).isEmpty();
    }

    @Test
    void shouldMoveChangeTimeWhenRowsDecrease() {
        // Given the final update reports rows written, which may be lower than rows read
        CompactionProgressMonitor monitor = new CompactionProgressMonitor(supplyTimes(
                Instant.parse("2026-10-26T10:00:05Z"),
                Instant.parse("2026-10-26T10:00:10Z")));

        // When
        monitor.accept(100);
        monitor.accept(80);

        // Then
        assertThat(monitor.getProgress()).contains(new CompactionProgress(80,
                Instant.parse("2026-10-26T10:00:10Z"),
                Instant.parse("2026-10-26T10:00:10Z")));
    }
}
