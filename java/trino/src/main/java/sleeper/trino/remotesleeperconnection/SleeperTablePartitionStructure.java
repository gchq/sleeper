package sleeper.trino.remotesleeperconnection;

import io.airlift.log.Logger;
import sleeper.core.partition.Partition;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

class SleeperTablePartitionStructure {
    private static final Logger LOG = Logger.get(SleeperTablePartitionStructure.class);

    private final Instant asOfInstant;
    private final List<Partition> allPartitions;
    private final Map<String, List<String>> partitionToFileMapping;

    public SleeperTablePartitionStructure(Instant asOfInstant,
                                          List<Partition> allPartitions,
                                          Map<String, List<String>> partitionToFileMapping) {
        this.asOfInstant = asOfInstant;
        this.allPartitions = allPartitions;
        this.partitionToFileMapping = partitionToFileMapping;
    }

    public Instant getAsOfInstant() {
        return asOfInstant;
    }

    public List<Partition> getAllPartitions() {
        return allPartitions;
    }

    public Map<String, List<String>> getPartitionToFileMapping() {
        return partitionToFileMapping;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SleeperTablePartitionStructure that = (SleeperTablePartitionStructure) o;
        return asOfInstant.equals(that.asOfInstant) && allPartitions.equals(that.allPartitions) && partitionToFileMapping.equals(that.partitionToFileMapping);
    }

    @Override
    public int hashCode() {
        return Objects.hash(asOfInstant, allPartitions, partitionToFileMapping);
    }

    @Override
    public String toString() {
        return "SleeperTablePartitionStructure{" +
                "retrievalInstant=" + asOfInstant +
                ", allPartitions=" + allPartitions +
                ", partitionToFileMapping=" + partitionToFileMapping +
                '}';
    }
}
