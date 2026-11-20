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
package sleeper.clients.report.filestatus;

import org.junit.jupiter.api.Test;

import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.job.InMemoryCompactionJobTracker;

import static org.assertj.core.api.Assertions.assertThat;

public class CompactionJobTrackerLookupTest {

    @Test
    void shouldHaveTrackerWhenCreatedFromTracker() {
        // When
        CompactionJobTrackerLookup lookup = CompactionJobTrackerLookup.from(new InMemoryCompactionJobTracker());

        // Then
        assertThat(lookup.isTrackerAvailable()).isTrue();
        assertThat(lookup.getJob("test-job")).isEmpty();
    }

    @Test
    void shouldHaveNoTrackerWhenTrackerIsDisabledForInstance() {
        // When
        CompactionJobTrackerLookup lookup = CompactionJobTrackerLookup.from(CompactionJobTracker.NONE);

        // Then
        assertThat(lookup.isTrackerAvailable()).isFalse();
        assertThat(lookup.getJob("test-job")).isEmpty();
    }

    @Test
    void shouldHaveNoTrackerWhenNoneRequested() {
        // When
        CompactionJobTrackerLookup lookup = CompactionJobTrackerLookup.none();

        // Then
        assertThat(lookup.isTrackerAvailable()).isFalse();
        assertThat(lookup.getJob("test-job")).isEmpty();
    }
}
