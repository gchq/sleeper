package sleeper.trino.systemtable;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.*;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import sleeper.trino.SleeperRecordCursor;

import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

public class SleeperSystemTableRandom implements SystemTable {
    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint) {
        List<Type> types = ImmutableList.of(VarcharType.VARCHAR, VarcharType.VARCHAR, VarcharType.VARCHAR);
        List<List<Object>> randomRows = IntStream.range(0, 10)
                .mapToObj(dummy -> ImmutableList.<Object>of(
                        UUID.randomUUID().toString(),
                        UUID.randomUUID().toString(),
                        UUID.randomUUID().toString()))
                .collect(ImmutableList.toImmutableList());

        return new SleeperRecordCursor("random", types, randomRows.stream());
    }

    @Override
    public Distribution getDistribution() {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata() {
        List<ColumnMetadata> columnMetadataInOrder = ImmutableList.<ColumnMetadata>builder()
                .add(ColumnMetadata.builder().setName("col1").setType(VarcharType.VARCHAR).build())
                .add(ColumnMetadata.builder().setName("col2").setType(VarcharType.VARCHAR).build())
                .add(ColumnMetadata.builder().setName("col3").setType(VarcharType.VARCHAR).build())
                .build();
        return new ConnectorTableMetadata(new SchemaTableName("system", "random"), columnMetadataInOrder);
    }
}
