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
package sleeper.core.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PollWithRetriesTest {

    private List<Duration> foundSleeps = new ArrayList<>();

    private PollWithRetries poll(Consumer<PollWithRetries.Builder> config) {
        PollWithRetries.Builder builder = PollWithRetries.builder()
                .sleepInInterval(millis -> foundSleeps.add(Duration.ofMillis(millis)));
        config.accept(builder);
        return builder.build();
    }

    @Nested
    @DisplayName("Poll until a condition is met")
    class PollUntilCondition {

        @Test
        void shouldRepeatPoll() throws Exception {
            // Given
            PollWithRetries poll = poll(builder -> builder.pollIntervalMillis(100).maxPolls(2));
            Iterator<Boolean> iterator = List.of(false, true).iterator();

            // When
            poll.pollUntil("iterator returns true", iterator::next);

            // Then
            assertThat(iterator).isExhausted();
            assertThat(foundSleeps).containsExactly(Duration.ofMillis(100));
        }

        @Test
        void shouldFinishIfMetOnFirstPoll() throws Exception {
            // Given
            PollWithRetries poll = poll(builder -> builder.pollIntervalMillis(100).maxPolls(2));
            Iterator<Boolean> iterator = List.of(true).iterator();

            // When
            poll.pollUntil("iterator returns true", iterator::next);

            // Then
            assertThat(iterator).isExhausted();
            assertThat(foundSleeps).isEmpty();
        }

        @Test
        void shouldFailIfMaxPollsReached() {
            // Given
            PollWithRetries poll = poll(builder -> builder.pollIntervalMillis(100).maxPolls(2));
            Iterator<Boolean> iterator = List.of(false, false).iterator();

            // When / Then
            assertThatThrownBy(() -> poll.pollUntil("iterator returns true", iterator::next))
                    .isInstanceOf(PollWithRetries.TimedOutException.class)
                    .hasMessage("Timed out after 2 tries waiting for 0.2s until iterator returns true");
            assertThat(iterator).isExhausted();
            assertThat(foundSleeps).containsExactly(Duration.ofMillis(100));
        }

        @Test
        void shouldResetPollCountBetweenPollUntilCalls() throws Exception {
            // Given
            PollWithRetries poll = poll(builder -> builder.pollIntervalMillis(100).maxPolls(2));
            Iterator<Boolean> iterator1 = List.of(false, true).iterator();
            Iterator<Boolean> iterator2 = List.of(false, true).iterator();

            // When
            poll.pollUntil("iterator returns true", iterator1::next);
            poll.pollUntil("iterator returns true", iterator2::next);

            // Then
            assertThat(iterator1).isExhausted();
            assertThat(iterator2).isExhausted();
            assertThat(foundSleeps).containsExactly(Duration.ofMillis(100), Duration.ofMillis(100));
        }
    }

    @Nested
    @DisplayName("Compute number of polls")
    class ComputePolls {

        @Test
        void shouldComputeMaxPollsFromTimeout() {
            assertThat(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(1), Duration.ofMinutes(1)))
                    .isEqualTo(PollWithRetries.intervalAndMaxPolls(1000, 60));
        }

        @Test
        void shouldComputeMaxPollsFromTimeoutWhichIsNotAnExactMultipleOfPollInterval() {
            assertThat(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(1), Duration.ofMillis(1500)))
                    .isEqualTo(PollWithRetries.intervalAndMaxPolls(1000, 2));
        }
    }

    @Nested
    @DisplayName("Query until the result meets a condition")
    class QueryUntilCondition {

        @Test
        void shouldRepeatQuery() throws Exception {
            // Given
            PollWithRetries poll = poll(builder -> builder.pollIntervalMillis(100).maxPolls(2));
            Iterator<String> iterator = List.of("a", "b").iterator();

            // When
            String result = poll.queryUntil("result is b", iterator::next, "b"::equals);

            // Then
            assertThat(iterator).isExhausted();
            assertThat(result).isEqualTo("b");
            assertThat(foundSleeps).containsExactly(Duration.ofMillis(100));
        }

        @Test
        void shouldFailSingleCheck() {
            // Given
            PollWithRetries poll = poll(builder -> builder.noRetries());
            Iterator<Boolean> iterator = List.of(false).iterator();

            // When / Then
            assertThatThrownBy(() -> poll.pollUntil("iterator returns true", iterator::next))
                    .isInstanceOf(PollWithRetries.CheckFailedException.class)
                    .hasMessage("Failed, expected to find iterator returns true");
            assertThat(iterator).isExhausted();
            assertThat(foundSleeps).isEmpty();
        }
    }

    @Nested
    @DisplayName("Consume max attempts over multiple invocations")
    class TrackOverallAttempts {

        @Test
        void shouldRefuseFurtherRetriesWhenConsumedByEarlierInvocation() throws Exception {
            // Given
            PollWithRetries poll = poll(builder -> builder.pollIntervalMillis(100)
                    .maxPolls(1).applyMaxPollsOverall());
            poll.pollUntil("true is returned", () -> true);

            // When / Then
            assertThatThrownBy(() -> poll.pollUntil("true is returned", () -> true))
                    .isInstanceOf(PollWithRetries.CheckFailedException.class);
            assertThat(foundSleeps).isEmpty();
        }

        @Test
        void shouldRefuseFurtherRetriesWhenPartlyConsumedByEarlierInvocation() throws Exception {
            // Given
            PollWithRetries poll = poll(builder -> builder.pollIntervalMillis(100)
                    .maxPolls(3).applyMaxPollsOverall());
            Iterator<Boolean> iterator1 = List.of(false, true).iterator();
            Iterator<Boolean> iterator2 = List.of(false).iterator();
            poll.pollUntil("true is returned", iterator1::next);

            // When / Then
            assertThatThrownBy(() -> poll.pollUntil("true is returned", iterator2::next))
                    .isInstanceOf(PollWithRetries.TimedOutException.class);
            assertThat(iterator1).isExhausted();
            assertThat(iterator2).isExhausted();
            assertThat(foundSleeps).containsExactly(Duration.ofMillis(100));
        }

        @Test
        void shouldAllowSuccessfulPollWhenPartlyConsumedByEarlierInvocation() throws Exception {
            // Given
            PollWithRetries poll = poll(builder -> builder.pollIntervalMillis(100)
                    .maxPolls(3).applyMaxPollsOverall());
            Iterator<Boolean> iterator1 = List.of(false, true).iterator();
            Iterator<Boolean> iterator2 = List.of(true).iterator();
            poll.pollUntil("true is returned", iterator1::next);

            // When / Then
            poll.pollUntil("true is returned", iterator2::next);

            // Then
            assertThat(iterator1).isExhausted();
            assertThat(iterator2).isExhausted();
            assertThat(foundSleeps).containsExactly(Duration.ofMillis(100));
        }

        @Test
        void shouldAllowSuccessfulPollWithRetryWhenPartlyConsumedByEarlierInvocation() throws Exception {
            // Given
            PollWithRetries poll = poll(builder -> builder.pollIntervalMillis(100)
                    .maxPolls(4).applyMaxPollsOverall());
            Iterator<Boolean> iterator1 = List.of(false, true).iterator();
            Iterator<Boolean> iterator2 = List.of(false, true).iterator();
            poll.pollUntil("true is returned", iterator1::next);

            // When / Then
            poll.pollUntil("true is returned", iterator2::next);

            // Then
            assertThat(iterator1).isExhausted();
            assertThat(iterator2).isExhausted();
            assertThat(foundSleeps).containsExactly(Duration.ofMillis(100), Duration.ofMillis(100));
        }

        @Test
        void shouldResetPollAttemptsOnCopy() throws Exception {
            // Given
            PollWithRetries poll = poll(builder -> builder.pollIntervalMillis(100)
                    .maxPolls(2).applyMaxPollsOverall());
            Iterator<Boolean> iterator1 = List.of(false, true).iterator();
            Iterator<Boolean> iterator2 = List.of(false, true).iterator();
            poll.pollUntil("true is returned", iterator1::next);

            // When
            poll.toBuilder().build().pollUntil("true is returned", iterator2::next);

            // Then
            assertThat(iterator1).isExhausted();
            assertThat(iterator2).isExhausted();
            assertThat(foundSleeps).containsExactly(Duration.ofMillis(100), Duration.ofMillis(100));
        }
    }
}
