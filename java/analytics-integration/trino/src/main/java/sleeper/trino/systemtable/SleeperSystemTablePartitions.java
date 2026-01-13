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
package sleeper.trino.systemtable;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import sleeper.trino.SleeperRecordCursor;
import sleeper.trino.remotesleeperconnection.SleeperConnectionAsTrino;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class SleeperSystemTablePartitions implements SystemTable {
    private static final String SCHEMA_NAME = "system";
    private static final String TABLE_NAME = "partitions";
    private static final ConnectorTableMetadata TABLE_METADATA = new ConnectorTableMetadata(
            new SchemaTableName(SCHEMA_NAME, TABLE_NAME),
            SleeperConnectionAsTrino.getPartitionStatusColumnMetadata());

    private final int schemaNameColumnIndex;
    private final int tableNameColumnIndex;

    private final SleeperConnectionAsTrino sleeperConnectionAsTrino;

    public SleeperSystemTablePartitions(SleeperConnectionAsTrino sleeperConnectionAsTrino) {
        this.sleeperConnectionAsTrino = requireNonNull(sleeperConnectionAsTrino);

        // Find the column indices for the schema and table columns.
        // This is a messy hack and these values should really be passed over from the SleeperConnectionAsTrino in some way.
        List<String> columnNames = TABLE_METADATA.getColumns().stream().map(ColumnMetadata::getName).collect(ImmutableList.toImmutableList());
        this.schemaNameColumnIndex = columnNames.indexOf("schemaname");
        this.tableNameColumnIndex = columnNames.indexOf("tablename");
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint) {
        Optional<Map<Integer, Domain>> columnIndexToDomainMapOpt = constraint.getDomains();
        if (!(columnIndexToDomainMapOpt.isPresent()
                && columnIndexToDomainMapOpt.get().containsKey(this.schemaNameColumnIndex)
                && columnIndexToDomainMapOpt.get().containsKey(this.tableNameColumnIndex))) {
            throw new UnsupportedOperationException("A predicate must be applied to the schemaname and tablename columns");
        }
        Domain schemaNameDomain = columnIndexToDomainMapOpt.get().get(this.schemaNameColumnIndex);
        Domain tableNameDomain = columnIndexToDomainMapOpt.get().get(this.tableNameColumnIndex);
        if (!(schemaNameDomain.isSingleValue() && tableNameDomain.isSingleValue())) {
            throw new UnsupportedOperationException("A single schema name and a single table name must be supplied as a predicate");
        }
        SchemaTableName schemaTableName = new SchemaTableName(
                ((Slice) schemaNameDomain.getSingleValue()).toStringUtf8(),
                ((Slice) tableNameDomain.getSingleValue()).toStringUtf8());

        return new SleeperRecordCursor(
                "partitions",
                TABLE_METADATA.getColumns().stream().map(ColumnMetadata::getType).collect(ImmutableList.toImmutableList()),
                this.sleeperConnectionAsTrino.streamPartitionStatusRows(schemaTableName));
    }

    @Override
    public Distribution getDistribution() {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata() {
        return TABLE_METADATA;
    }
}
