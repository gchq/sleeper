package sleeper.trino.handle;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;
import sleeper.query.model.LeafPartitionQuery;
import sleeper.query.model.QuerySerDe;

import java.util.List;
import java.util.Objects;

/**
 * This class holds a description of a single Trino split. In this implementation, each split holds one or more
 * range-scans over a single Sleeper leaf partition, as described in the {@link LeafPartitionQuery} class.
 * <p>
 * Trino requires that class is serialisable into JSON, and this causes issues when fields such as the row keys are
 * defined as generic Objects. When these generic Objects are deserialised, the class information is not retained.
 * Jackson does provide ways to retain this class information, but these did not seem to work on Lists-of-Objects,
 * possibly because of type erasure. Instead, this class has JSON properties which are all based on Strings, and
 * performs its own conversion to specific types. Methods are provided to work with the String and non-String versions.
 */
public class SleeperSplit implements ConnectorSplit {

    private final Schema sleeperSchema;
    private final LeafPartitionQuery leafPartitionQuery;

    /**
     * Constructor to create a {@link SleeperSplit} directly from a {@link LeafPartitionQuery}.
     *
     * @param leafPartitionQuery The {@link LeafPartitionQuery} to use to construct this split.
     */
    public SleeperSplit(Schema sleeperSchema, LeafPartitionQuery leafPartitionQuery) {
        this.sleeperSchema = sleeperSchema;
        this.leafPartitionQuery = leafPartitionQuery;
    }

    /**
     * Constructor using string-based data.
     * <p>
     * This method supports deserialisation from JSON. During the serialisation/deserialisation process, those getter
     * methods which are annotated with 'JsonProperty' are called and their values are placed into the serialised JSON.
     * During deserialisation, those values are passed from that JSON into this constructor, where the JsonProperty
     * matches the name of the getter that originally supplied the values.
     */
    @JsonCreator
    public SleeperSplit(@JsonProperty("tableName") String tableName,
                        @JsonProperty("sleeperSchemaAsString") String sleeperSchemaAsString,
                        @JsonProperty("leafPartitionQueryAsString") String leafPartitionQueryAsString) {
        SchemaSerDe schemaSerDe = new SchemaSerDe();
        this.sleeperSchema = schemaSerDe.fromJson(sleeperSchemaAsString);
        QuerySerDe querySerDe = new QuerySerDe(ImmutableMap.of(tableName, this.sleeperSchema));
        this.leafPartitionQuery = (LeafPartitionQuery) querySerDe.fromJson(leafPartitionQueryAsString);
    }

    @JsonProperty
    public String getTableName() {
        return this.getLeafPartitionQuery().getTableName();
    }

    public Schema getSleeperSchema() {
        return this.sleeperSchema;
    }

    @JsonProperty
    public String getSleeperSchemaAsString() {
        SchemaSerDe schemaSerDe = new SchemaSerDe();
        return schemaSerDe.toJson(this.getSleeperSchema());
    }

    public LeafPartitionQuery getLeafPartitionQuery() {
        return this.leafPartitionQuery;
    }

    @JsonProperty
    public String getLeafPartitionQueryAsString() {
        QuerySerDe querySerDe = new QuerySerDe(ImmutableMap.of(this.getTableName(), this.getSleeperSchema()));
        return querySerDe.toJson(this.getLeafPartitionQuery());
    }

    /**
     * The splits retrieve the data that they need from DynamoDB and S3, and so they are not tied to a specific host.
     *
     * @return True
     */
    @Override
    public boolean isRemotelyAccessible() {
        return true;
    }

    /**
     * Return a list of host addresses where this split can run. The splits retrieve the data that they need from
     * DynamoDB and S3, and so they are not tied to a specific host. Returning an empty list indicates that it does not
     * matter which host the split runs on.
     *
     * @return An empty list.
     */
    @Override
    public List<HostAddress> getAddresses() {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo() {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SleeperSplit that = (SleeperSplit) o;
        return sleeperSchema.equals(that.sleeperSchema) && leafPartitionQuery.equals(that.leafPartitionQuery);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sleeperSchema, leafPartitionQuery);
    }

    @Override
    public String toString() {
        return "SleeperSplit{" +
                "sleeperSchema=" + sleeperSchema +
                ", leafPartitionQuery=" + leafPartitionQuery +
                '}';
    }
}
