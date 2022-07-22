package sleeper.trino.handle;

import io.trino.spi.connector.ConnectorPartitionHandle;
import sleeper.core.key.Key;

import java.util.Objects;

/**
 * This class holds the details about a single Trino partition.
 * <p>
 * In this implementation, a Trino partition is described by its lower-bound.
 */
public class SleeperPartitionHandle extends ConnectorPartitionHandle {
    private final Key trinoPartitionMin; // Inclusive

    public SleeperPartitionHandle(Key trinoPartitionMin) {
        this.trinoPartitionMin = trinoPartitionMin;
    }

    public Key getTrinoPartitionMin() {
        return trinoPartitionMin;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SleeperPartitionHandle that = (SleeperPartitionHandle) o;
        return Objects.equals(trinoPartitionMin, that.trinoPartitionMin);
    }

    @Override
    public int hashCode() {
        return Objects.hash(trinoPartitionMin);
    }

    @Override
    public String toString() {
        return "SleeperPartitionHandle{" +
                "trinoPartitionMin=" + trinoPartitionMin +
                '}';
    }
}
