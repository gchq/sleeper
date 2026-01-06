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
import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.QuerySerDe;

import java.util.List;
import java.util.Objects;

/**
 * Holds a description of a single Trino split. In this implementation, each split holds one or more range-scans over a
 * single Sleeper leaf partition, as described in the {@link LeafPartitionQuery} class.
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
     * Constructor to create a split directly from a subquery.
     *
     * @param sleeperSchema      the Sleeper table schema
     * @param leafPartitionQuery the query to use to construct this split
     */
    public SleeperSplit(Schema sleeperSchema, LeafPartitionQuery leafPartitionQuery) {
        this.sleeperSchema = sleeperSchema;
        this.leafPartitionQuery = leafPartitionQuery;
    }

    /**
     * Constructor using string-based data. This method supports deserialisation from JSON.
     * <p>
     * During the serialisation/deserialisation process, those getter methods which are annotated with 'JsonProperty'
     * are called and their values are placed into the serialised JSON. During deserialisation, those values are passed
     * from that JSON into this constructor, where the JsonProperty matches the name of the getter that originally
     * supplied the values.
     *
     * @param sleeperSchemaAsString      the Sleeper table schema, serialised as a string
     * @param leafPartitionQueryAsString the query to use to construct this split, serialised as a string
     */
    @SuppressFBWarnings("BC_UNCONFIRMED_CAST_OF_RETURN_VALUE")
    @JsonCreator
    public SleeperSplit(@JsonProperty("sleeperSchemaAsString") String sleeperSchemaAsString,
            @JsonProperty("leafPartitionQueryAsString") String leafPartitionQueryAsString) {
        SchemaSerDe schemaSerDe = new SchemaSerDe();
        this.sleeperSchema = schemaSerDe.fromJson(sleeperSchemaAsString);
        QuerySerDe querySerDe = new QuerySerDe(sleeperSchema);
        this.leafPartitionQuery = querySerDe.fromJsonOrLeafQuery(leafPartitionQueryAsString).asLeafQuery();
    }

    public Schema getSleeperSchema() {
        return sleeperSchema;
    }

    @JsonProperty
    public String getSleeperSchemaAsString() {
        SchemaSerDe schemaSerDe = new SchemaSerDe();
        return schemaSerDe.toJson(getSleeperSchema());
    }

    public LeafPartitionQuery getLeafPartitionQuery() {
        return leafPartitionQuery;
    }

    @JsonProperty
    public String getLeafPartitionQueryAsString() {
        QuerySerDe querySerDe = new QuerySerDe(getSleeperSchema());
        return querySerDe.toJson(getLeafPartitionQuery());
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
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
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
