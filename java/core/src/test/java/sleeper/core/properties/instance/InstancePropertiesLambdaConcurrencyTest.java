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
package sleeper.core.properties.instance;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import sleeper.core.properties.SleeperPropertiesValidationReporter;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CommonProperty.DEFAULT_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_COMMIT_BATCHER_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_COMMIT_BATCHER_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_DISPATCH_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_DISPATCH_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.GarbageCollectionProperty.GARBAGE_COLLECTOR_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.GarbageCollectionProperty.GARBAGE_COLLECTOR_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.MetricsProperty.METRICS_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.MetricsProperty.METRICS_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.PartitionSplittingProperty.FIND_PARTITIONS_TO_SPLIT_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.PartitionSplittingProperty.FIND_PARTITIONS_TO_SPLIT_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.PartitionSplittingProperty.SPLIT_PARTITIONS_RESERVED_CONCURRENCY;
import static sleeper.core.properties.instance.TableStateProperty.SNAPSHOT_CREATION_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.TableStateProperty.SNAPSHOT_CREATION_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.TableStateProperty.SNAPSHOT_DELETION_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.TableStateProperty.SNAPSHOT_DELETION_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.TableStateProperty.STATESTORE_COMMITTER_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.TableStateProperty.STATESTORE_COMMITTER_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.TableStateProperty.TRANSACTION_DELETION_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.TableStateProperty.TRANSACTION_DELETION_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.TableStateProperty.TRANSACTION_FOLLOWER_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

class InstancePropertiesLambdaConcurrencyTest {
    private final InstanceProperties properties = createTestInstanceProperties();

    @ParameterizedTest
    @MethodSource("sqsConcurrencyProperties")
    void shouldRejectMaximumAboveReserved(InstanceProperty reserved, InstanceProperty maximum) {
        // Given
        properties.setNumber(reserved, 2);
        properties.setNumber(maximum, 3);
        SleeperPropertiesValidationReporter reporter = new SleeperPropertiesValidationReporter();

        // When
        properties.validate(reporter);

        // Then
        assertThat(reporter.isValid(reserved)).isFalse();
        assertThat(reporter.isValid(maximum)).isFalse();
    }

    @ParameterizedTest
    @MethodSource("sqsConcurrencyProperties")
    void shouldAcceptEqualMaximumAndReserved(InstanceProperty reserved, InstanceProperty maximum) {
        // Given
        properties.setNumber(reserved, 10);
        properties.setNumber(maximum, 10);

        // When / Then
        assertThat(properties.isValid()).isTrue();
    }

    @ParameterizedTest
    @MethodSource("sqsConcurrencyProperties")
    void shouldAcceptMaximumBelowReserved(InstanceProperty reserved, InstanceProperty maximum) {
        // Given
        properties.setNumber(reserved, 20);
        properties.setNumber(maximum, 10);

        // When / Then
        assertThat(properties.isValid()).isTrue();
    }

    @ParameterizedTest
    @MethodSource("sqsConcurrencyProperties")
    void shouldRejectMaximumAboveServiceLimit(InstanceProperty reserved, InstanceProperty maximum) {
        // Given
        properties.setNumber(reserved, 2000);
        properties.setNumber(maximum, 1001);

        // When / Then
        assertThat(properties.isValid()).isFalse();
    }

    @Test
    void shouldRejectInheritedMaximumAboveCommitterReserved() {
        // Given
        properties.setNumber(DEFAULT_LAMBDA_CONCURRENCY_MAXIMUM, 20);
        SleeperPropertiesValidationReporter reporter = new SleeperPropertiesValidationReporter();

        // When
        properties.validate(reporter);

        // Then
        assertThat(reporter.isValid(STATESTORE_COMMITTER_LAMBDA_CONCURRENCY_RESERVED)).isFalse();
        assertThat(reporter.isValid(STATESTORE_COMMITTER_LAMBDA_CONCURRENCY_MAXIMUM)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"invalid", "-1"})
    void shouldReportUnreadableOrNegativeCommitterReserved(String value) {
        // Given
        properties.set(STATESTORE_COMMITTER_LAMBDA_CONCURRENCY_RESERVED, value);

        // When / Then
        assertThat(properties.isValid()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"invalid", "1", "1001"})
    void shouldReportInvalidCommitterMaximum(String value) {
        // Given
        properties.set(STATESTORE_COMMITTER_LAMBDA_CONCURRENCY_MAXIMUM, value);

        // When / Then
        assertThat(properties.isValid()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void shouldAllowReservedConcurrencyBelowTwoWithoutAnSqsMaximum(int reserved) {
        // Given - the transaction follower is driven by DynamoDB, not an SQS mapping
        properties.setNumber(TRANSACTION_FOLLOWER_LAMBDA_CONCURRENCY_RESERVED, reserved);

        // When / Then
        assertThat(properties.isValid()).isTrue();
    }

    @Test
    void shouldAcceptDefaultsWithoutReservedConcurrencyForMostFunctions() {
        assertThat(properties.isValid()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 1000})
    void shouldAcceptSqsMaximumAtServiceBoundaries(int maximum) {
        // Given
        properties.setNumber(STATESTORE_COMMITTER_LAMBDA_CONCURRENCY_RESERVED, maximum);
        properties.setNumber(STATESTORE_COMMITTER_LAMBDA_CONCURRENCY_MAXIMUM, maximum);

        // When / Then
        assertThat(properties.isValid()).isTrue();
    }

    @Test
    void shouldReportUnreadableInheritedMaximumWithoutThrowing() {
        // Given
        properties.set(DEFAULT_LAMBDA_CONCURRENCY_MAXIMUM, "invalid");

        // When / Then
        assertThat(properties.isValid()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"invalid", "1", "1001"})
    void shouldRejectInvalidSharedPartitionSplittingConcurrency(String value) {
        // Given - this property sets both reserved and SQS maximum concurrency
        properties.set(SPLIT_PARTITIONS_RESERVED_CONCURRENCY, value);

        // When / Then
        assertThat(properties.isValid()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 1000})
    void shouldAcceptSharedPartitionSplittingConcurrencyAtServiceBoundaries(int concurrency) {
        // Given
        properties.setNumber(SPLIT_PARTITIONS_RESERVED_CONCURRENCY, concurrency);

        // When / Then
        assertThat(properties.isValid()).isTrue();
    }

    private static Stream<Arguments> sqsConcurrencyProperties() {
        return Stream.of(
                Arguments.of(METRICS_LAMBDA_CONCURRENCY_RESERVED, METRICS_LAMBDA_CONCURRENCY_MAXIMUM),
                Arguments.of(GARBAGE_COLLECTOR_LAMBDA_CONCURRENCY_RESERVED, GARBAGE_COLLECTOR_LAMBDA_CONCURRENCY_MAXIMUM),
                Arguments.of(FIND_PARTITIONS_TO_SPLIT_LAMBDA_CONCURRENCY_RESERVED, FIND_PARTITIONS_TO_SPLIT_LAMBDA_CONCURRENCY_MAXIMUM),
                Arguments.of(COMPACTION_JOB_CREATION_LAMBDA_CONCURRENCY_RESERVED, COMPACTION_JOB_CREATION_LAMBDA_CONCURRENCY_MAXIMUM),
                Arguments.of(COMPACTION_JOB_DISPATCH_LAMBDA_CONCURRENCY_RESERVED, COMPACTION_JOB_DISPATCH_LAMBDA_CONCURRENCY_MAXIMUM),
                Arguments.of(COMPACTION_COMMIT_BATCHER_LAMBDA_CONCURRENCY_RESERVED, COMPACTION_COMMIT_BATCHER_LAMBDA_CONCURRENCY_MAXIMUM),
                Arguments.of(STATESTORE_COMMITTER_LAMBDA_CONCURRENCY_RESERVED, STATESTORE_COMMITTER_LAMBDA_CONCURRENCY_MAXIMUM),
                Arguments.of(SNAPSHOT_CREATION_LAMBDA_CONCURRENCY_RESERVED, SNAPSHOT_CREATION_LAMBDA_CONCURRENCY_MAXIMUM),
                Arguments.of(SNAPSHOT_DELETION_LAMBDA_CONCURRENCY_RESERVED, SNAPSHOT_DELETION_LAMBDA_CONCURRENCY_MAXIMUM),
                Arguments.of(TRANSACTION_DELETION_LAMBDA_CONCURRENCY_RESERVED, TRANSACTION_DELETION_LAMBDA_CONCURRENCY_MAXIMUM));
    }
}
