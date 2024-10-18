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

package sleeper.statestore.s3;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.statestore.StateStoreException;
import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.core.util.ThreadSleepTestHelper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.DoubleSupplier;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.statestore.s3.InMemoryS3StateStoreDataFiles.buildPathFromRevisionId;
import static sleeper.statestore.s3.S3StateStoreDataFile.conditionCheckFor;

public class S3StateStoreDataFileTest {

    private static final String REVISION_ID = "objects";
    private static final String INITIAL_DATA = "test initial data";

    private final InMemoryS3RevisionIdStore revisionStore = new InMemoryS3RevisionIdStore();
    private final InMemoryS3StateStoreDataFiles<Object> dataFiles = new InMemoryS3StateStoreDataFiles<>();
    private final List<Duration> foundWaits = new ArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        S3RevisionId firstRevision = S3RevisionId.firstRevision("first");
        revisionStore.initialise(REVISION_ID, firstRevision);
        dataFiles.write(INITIAL_DATA, buildPathFromRevisionId(firstRevision));
    }

    @Test
    void shouldUpdateOnFirstAttempt() throws Exception {
        // When
        updateWithAttempts(1, existing -> "new", existing -> "");

        // Then
        assertThat(loadCurrentData()).isEqualTo("new");
        assertThat(foundWaits).isEmpty();
    }

    @Test
    void shouldSeeExistingDataWhenUpdating() throws Exception {
        // When
        List<Object> foundExistingOnUpdate = new ArrayList<>();
        List<Object> foundExistingOnCondition = new ArrayList<>();
        updateWithAttempts(1, existing -> {
            foundExistingOnUpdate.add(existing);
            return "new";
        }, existing -> {
            foundExistingOnCondition.add(existing);
            return "";
        });

        // Then
        assertThat(foundExistingOnUpdate).containsExactly(INITIAL_DATA);
        assertThat(foundExistingOnCondition).containsExactly(INITIAL_DATA);
    }

    @Test
    void shouldFailUpdateWithNoFurtherAttemptsWhenConditionFails() throws Exception {
        // When / Then
        assertThatThrownBy(() -> updateWithAttempts(10, existing -> "willNotHappen", existing -> "test condition failure"))
                .isInstanceOf(StateStoreException.class)
                .hasMessageContaining("test condition failure");
        assertThat(loadCurrentData()).isEqualTo(INITIAL_DATA);
        assertThat(foundWaits).isEmpty();
    }

    @Test
    void shouldUpdateAfter10Attempts() throws Exception {
        // Given data is updated in contention until after 9 attempts
        setDataInContentionAfterQueries(
                List.of("update-1", "update-2", "update-3", "update-4", "update-5",
                        "update-6", "update-7", "update-8", "update-9"));

        // When 10 attempts are allowed
        updateWithAttempts(10, existing -> "new", existing -> "");

        // Then the update succeeds with 9 unsuccessful attempts
        assertThat(loadCurrentData()).isEqualTo("new");
        assertThat(foundWaits).containsExactly(
                Duration.parse("PT2.923S"),
                Duration.parse("PT1.924S"),
                Duration.parse("PT10.198S"),
                Duration.parse("PT17.613S"),
                Duration.parse("PT38.242S"),
                Duration.parse("PT39.986S"),
                Duration.parse("PT46.222S"),
                Duration.parse("PT1M58.18S"),
                Duration.parse("PT1M45.501S"));
    }

    @Test
    void shouldUpdateAfter10AttemptsWithNoJitter() throws Exception {
        // Given data is updated in contention until after 9 attempts
        setDataInContentionAfterQueries(
                List.of("update-1", "update-2", "update-3", "update-4", "update-5",
                        "update-6", "update-7", "update-8", "update-9"));

        // When 10 attempts are allowed
        updateWithFullJitterFractionAndAttempts(noJitter(), 10, existing -> "new", existing -> "");

        // Then the update succeeds with 9 unsuccessful attempts
        assertThat(loadCurrentData()).isEqualTo("new");
        assertThat(foundWaits).containsExactly(
                Duration.ofSeconds(4),
                Duration.ofSeconds(8),
                Duration.ofSeconds(16),
                Duration.ofSeconds(32),
                Duration.ofSeconds(64),
                Duration.ofMinutes(2),
                Duration.ofMinutes(2),
                Duration.ofMinutes(2),
                Duration.ofMinutes(2));
    }

    @Test
    void shouldUpdateAfter10AttemptsWithConstantJitterFraction() throws Exception {
        // Given data is updated in contention until after 9 attempts
        setDataInContentionAfterQueries(
                List.of("update-1", "update-2", "update-3", "update-4", "update-5",
                        "update-6", "update-7", "update-8", "update-9"));

        // When 10 attempts are allowed
        updateWithFullJitterFractionAndAttempts(
                constantJitterFraction(0.5), 10, existing -> "new", existing -> "");

        // Then the update succeeds with 9 unsuccessful attempts
        assertThat(loadCurrentData()).isEqualTo("new");
        assertThat(foundWaits).containsExactly(
                Duration.ofSeconds(2),
                Duration.ofSeconds(4),
                Duration.ofSeconds(8),
                Duration.ofSeconds(16),
                Duration.ofSeconds(32),
                Duration.ofMinutes(1),
                Duration.ofMinutes(1),
                Duration.ofMinutes(1),
                Duration.ofMinutes(1));
    }

    @Test
    void shouldFailUpdateWhenTooManyAttemptsWereMade() throws Exception {
        // Given data is updated in contention until after 3 attempts
        setDataInContentionAfterQueries(
                List.of("update-1", "update-2", "update-3"));

        // When 2 attempts are allowed
        // Then the update fails
        assertThatThrownBy(() -> updateWithAttempts(2, existing -> "updated", existing -> ""))
                .isInstanceOf(StateStoreException.class)
                .hasMessage("Too many update attempts, failed after 2 attempts");
        assertThat(loadCurrentData()).isEqualTo("update-2");
        assertThat(foundWaits).hasSize(1);
    }

    @Test
    void shouldRetryWhenLoadingDataFails() throws Exception {
        // Given
        dataFiles.setFailureOnNextDataLoad("Failed loading test data");

        // When
        updateWithAttempts(2, existing -> "updated", existing -> "");

        // Then
        assertThat(loadCurrentData()).isEqualTo("updated");
        assertThat(foundWaits).hasSize(1);
    }

    @Test
    void shouldFailWhenLoadingDataFailsOnLastAttempt() throws Exception {
        // Given
        dataFiles.setFailureOnNextDataLoad("Failed loading test data");

        // When / Then
        assertThatThrownBy(() -> updateWithAttempts(1, existing -> "updated", existing -> ""))
                .isInstanceOf(StateStoreException.class)
                .hasMessage("Too many update attempts, failed after 1 attempts");
        assertThat(loadCurrentData()).isEqualTo(INITIAL_DATA);
        assertThat(foundWaits).isEmpty();
    }

    @Test
    void shouldRetryWhenWritingDataFails() throws Exception {
        // Given
        dataFiles.setFailureOnNextDataWrite("Failed writing test data");

        // When
        updateWithAttempts(2, existing -> "updated", existing -> "");

        // Then
        assertThat(loadCurrentData()).isEqualTo("updated");
        assertThat(foundWaits).hasSize(1);
    }

    @Test
    void shouldFailWhenWritingDataFailsOnLastAttempt() throws Exception {
        // Given
        dataFiles.setFailureOnNextDataWrite("Failed writing test data");

        // When / Then
        assertThatThrownBy(() -> updateWithAttempts(1, existing -> "updated", existing -> ""))
                .isInstanceOf(StateStoreException.class)
                .hasMessage("Too many update attempts, failed after 1 attempts");
        assertThat(loadCurrentData()).isEqualTo(INITIAL_DATA);
        assertThat(foundWaits).isEmpty();
    }

    private void updateWithAttempts(int attempts, Function<Object, Object> update, Function<Object, String> condition) throws Exception {
        updateWithFullJitterFractionAndAttempts(randomSeededJitterFraction(0), attempts, update, condition);
    }

    private void updateWithFullJitterFractionAndAttempts(
            DoubleSupplier jitterFractionSupplier, int attempts,
            Function<Object, Object> update, Function<Object, String> condition) throws Exception {
        S3StateStoreDataFile.builder()
                .description("object")
                .revisionIdKey(REVISION_ID)
                .loadRevisionId(revisionStore::getCurrentRevisionId)
                .updateRevisionId(revisionStore::conditionalUpdateOfRevisionId)
                .buildPathFromRevisionId(InMemoryS3StateStoreDataFiles::buildPathFromRevisionId)
                .loadAndWriteData(dataFiles::load, dataFiles::write)
                .deleteFile(dataFiles::delete)
                .retryBackoff(new ExponentialBackoffWithJitter(
                        S3StateStoreDataFile.RETRY_WAIT_RANGE, jitterFractionSupplier, ThreadSleepTestHelper.recordWaits(foundWaits)))
                .build().updateWithAttempts(attempts, update, conditionCheckFor(condition));
    }

    private void setDataInContentionAfterQueries(List<Object> data) {
        revisionStore.setRevisionUpdatesInContentionAfterQueries(REVISION_ID,
                data.stream().map(this::setData).iterator());
    }

    private Consumer<S3RevisionId> setData(Object data) {
        return revisionId -> {
            try {
                dataFiles.write(data, buildPathFromRevisionId(revisionId));
            } catch (StateStoreException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private Object loadCurrentData() throws Exception {
        S3RevisionId revisionId = revisionStore.getCurrentRevisionId(REVISION_ID);
        return dataFiles.load("files/" + revisionId.getUuid());
    }

    private static DoubleSupplier noJitter() {
        return () -> 1.0;
    }

    private static DoubleSupplier constantJitterFraction(double fraction) {
        return () -> fraction;
    }

    private static DoubleSupplier randomSeededJitterFraction(int seed) {
        Random random = new Random(seed);
        return random::nextDouble;
    }
}
