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
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
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
