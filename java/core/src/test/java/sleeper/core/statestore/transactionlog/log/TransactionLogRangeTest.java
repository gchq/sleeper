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
package sleeper.core.statestore.transactionlog.log;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TransactionLogRangeTest {

    @Test
    void shouldRestrictMinimumToHigherNumber() {
        TransactionLogRange range = new TransactionLogRange(1, 5);

        assertThat(range.withMinTransactionNumber(3))
                .contains(new TransactionLogRange(3, 5));
    }

    @Test
    void shouldNotRestrictMinimumToLowerNumber() {
        TransactionLogRange range = new TransactionLogRange(4, 5);

        assertThat(range.withMinTransactionNumber(3))
                .contains(new TransactionLogRange(4, 5));
    }

    @Test
    void shouldRestrictMinimumToEmptyRangeWhenMinimumIsExactlyEnd() {
        TransactionLogRange range = new TransactionLogRange(1, 5);

        assertThat(range.withMinTransactionNumber(5))
                .isEmpty();
    }

    @Test
    void shouldRestrictMinimumToEmptyRangeWhenMinimumIsBeyondEnd() {
        TransactionLogRange range = new TransactionLogRange(1, 5);

        assertThat(range.withMinTransactionNumber(6))
                .isEmpty();
    }

    @Test
    void shouldRestrictMinimumForRangeWithUnboundedEnd() {
        TransactionLogRange range = TransactionLogRange.fromMinimum(1);

        assertThat(range.withMinTransactionNumber(6))
                .contains(TransactionLogRange.fromMinimum(6));
    }
}
