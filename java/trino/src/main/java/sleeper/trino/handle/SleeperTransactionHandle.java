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
package sleeper.trino.handle;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTransactionHandle;

import java.time.Instant;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * This handle holds details about the instant when the transaction was started.
 */
public class SleeperTransactionHandle implements ConnectorTransactionHandle {
    private final Instant transactionStartInstant;

    @JsonCreator
    public SleeperTransactionHandle(@JsonProperty("transactionStartInstant") Instant transactionStartInstant) {
        this.transactionStartInstant = requireNonNull(transactionStartInstant, "transactionStartInstant is null");
    }

    @JsonProperty
    public Instant getTransactionStartInstant() {
        return transactionStartInstant;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SleeperTransactionHandle that = (SleeperTransactionHandle) o;
        return transactionStartInstant == that.transactionStartInstant;
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionStartInstant);
    }

    @Override
    public String toString() {
        return "SleeperTransactionHandle{" +
                "transactionStartInstant=" + transactionStartInstant +
                '}';
    }
}
