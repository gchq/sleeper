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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Holds details to filter the results from the table when it is scanned. This includes the table name, the column
 * handles and any tuple domain.
 */
public class SleeperTableHandle implements ConnectorTableHandle {
    private final SchemaTableName schemaTableName;
    private final List<SleeperColumnHandle> sleeperColumnHandleListInOrder;
    private final TupleDomain<ColumnHandle> tupleDomain;

    @JsonCreator
    public SleeperTableHandle(@JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("sleeperColumnHandleListInOrder") List<SleeperColumnHandle> sleeperColumnHandleListInOrder,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain) {
        this.schemaTableName = requireNonNull(schemaTableName);
        this.sleeperColumnHandleListInOrder = requireNonNull(sleeperColumnHandleListInOrder);
        this.tupleDomain = requireNonNull(tupleDomain);
    }

    public SleeperTableHandle(SchemaTableName schemaTableName,
            List<SleeperColumnHandle> sleeperColumnHandleListInOrder) {
        this(schemaTableName, sleeperColumnHandleListInOrder, TupleDomain.all());
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName() {
        return schemaTableName;
    }

    @JsonProperty
    public List<SleeperColumnHandle> getSleeperColumnHandleListInOrder() {
        return sleeperColumnHandleListInOrder;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain() {
        return tupleDomain;
    }

    /**
     * A convenience method to return this handle as a table metadata object.
     *
     * @return the {@link ConnectorTableMetadata} object
     */
    public ConnectorTableMetadata toConnectorTableMetadata() {
        List<ColumnMetadata> columnMetadataList = this.sleeperColumnHandleListInOrder.stream()
                .map(SleeperColumnHandle::toColumnMetadata)
                .collect(ImmutableList.toImmutableList());
        return new ConnectorTableMetadata(this.schemaTableName, columnMetadataList);
    }

    /**
     * A convenience method to return this handle as a columns metadata object.
     *
     * @return the {@link TableColumnsMetadata} object
     */
    public TableColumnsMetadata toTableColumnsMetadata() {
        return TableColumnsMetadata.forTable(schemaTableName, toConnectorTableMetadata().getColumns());
    }

    /**
     * Copy this handle, replacing the tuple domain but keeping other fields intact.
     *
     * @param  newTupleDomain the {@link TupleDomain} to use in the new copy
     * @return                the copied {@link SleeperTableHandle}
     */
    public SleeperTableHandle withTupleDomain(TupleDomain<ColumnHandle> newTupleDomain) {
        return new SleeperTableHandle(schemaTableName, sleeperColumnHandleListInOrder, newTupleDomain);
    }

    public List<SleeperColumnHandle> getColumnHandlesInCategoryInOrder(SleeperColumnHandle.SleeperColumnCategory category) {
        return this.getSleeperColumnHandleListInOrder()
                .stream()
                .filter(sleeperColumnHandle -> sleeperColumnHandle.getColumnCategory() == category)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SleeperTableHandle that = (SleeperTableHandle) o;
        return Objects.equals(schemaTableName, that.schemaTableName) &&
                Objects.equals(sleeperColumnHandleListInOrder, that.sleeperColumnHandleListInOrder) &&
                Objects.equals(tupleDomain, that.tupleDomain);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaTableName, sleeperColumnHandleListInOrder, tupleDomain);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("schemaTableName", schemaTableName)
                .add("sleeperColumnHandleList", sleeperColumnHandleListInOrder)
                .add("tupleDomain", tupleDomain)
                .toString();
    }
}
