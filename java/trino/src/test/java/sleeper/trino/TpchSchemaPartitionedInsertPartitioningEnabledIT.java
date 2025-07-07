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
package sleeper.trino;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import sleeper.core.partition.Partition;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.StateStore;
import sleeper.trino.testutils.PopulatedSleeperExternalResource;

import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for inserts. This is a copy of {@link TpchSchemaPartitionedInsertPartitioningDisabledIT} with minor changes and
 * it would be a good idea to parameterise it if possible.
 */
public class TpchSchemaPartitionedInsertPartitioningEnabledIT {
    private static final String TPCH_TABLE_SIZE = "sf1";
    private static final int NO_OF_NATION_KEYS = 25;

    private static final List<PopulatedSleeperExternalResource.TableDefinition> TABLE_DEFINITIONS = ImmutableList.of(
            new PopulatedSleeperExternalResource.TableDefinition(
                    "customer_unpartitioned",
                    generateCustomerSchemaForPartitioning(),
                    List.of(),
                    Stream.empty()),
            new PopulatedSleeperExternalResource.TableDefinition(
                    "customer_partitioned",
                    generateCustomerSchemaForPartitioning(),
                    generateCustomerPartitionBoundaries(),
                    Stream.empty()));

    private static final Map<String, String> EXTRA_PROPERTIES_FOR_QUERY_RUNNER = ImmutableMap.of("task.max-writer-count", "4");
    private static final SleeperConfig SLEEPER_CONFIG = (new SleeperConfig()).setEnableTrinoPartitioning(true);
    @RegisterExtension
    public static final PopulatedSleeperExternalResource POPULATED_SLEEPER_EXTERNAL_RESOURCE = new PopulatedSleeperExternalResource(EXTRA_PROPERTIES_FOR_QUERY_RUNNER, TABLE_DEFINITIONS,
            SLEEPER_CONFIG);

    private static QueryAssertions assertions;

    @BeforeAll
    public static void init() {
        assertions = POPULATED_SLEEPER_EXTERNAL_RESOURCE.getQueryAssertions();
        assertions.execute(
                String.format("INSERT INTO sleeper.default.customer_unpartitioned " +
                        "SELECT nationkey, custkey, name, address, phone, CAST(acctbal AS VARCHAR), mktsegment, comment " +
                        "FROM tpch.%s.customer", TPCH_TABLE_SIZE));
        assertions.execute(
                String.format("INSERT INTO sleeper.default.customer_partitioned " +
                        "SELECT nationkey, custkey, name, address, phone, CAST(acctbal AS VARCHAR), mktsegment, comment " +
                        "FROM tpch.%s.customer", TPCH_TABLE_SIZE));
    }

    private static Schema generateCustomerSchemaForPartitioning() {
        // In these tests, the customer schema is keyed by the nation key. This is because the TPCH data-generator
        // produces its data pre-sorted by custkey (and hence by name) which means that partitioning on one of these
        // columns would automatically result in efficient writing of data. Nationkey is assigned to the data in
        // a pseudorandom way.
        return Schema.builder()
                .rowKeyFields(new Field("nationkey", new LongType()))
                .valueFields(
                        new Field("custkey", new LongType()),
                        new Field("name", new StringType()),
                        new Field("address", new StringType()),
                        new Field("phone", new StringType()),
                        new Field("acctbal", new StringType()),
                        new Field("mktsegment", new StringType()),
                        new Field("comment", new StringType()))
                .build();
    }

    private static List<Object> generateCustomerPartitionBoundaries() {
        return LongStream.range(0, NO_OF_NATION_KEYS).boxed().collect(ImmutableList.toImmutableList());
    }

    @Test
    public void testEq() {
        assertThat(assertions.query(
                "SELECT MIN(nationkey), MAX(nationkey) FROM customer_unpartitioned WHERE nationkey = 3"))
                .matches("VALUES (CAST(3 AS BIGINT), CAST(3 AS BIGINT))");
        assertThat(assertions.query(
                "SELECT MIN(nationkey), MAX(nationkey) FROM customer_partitioned WHERE nationkey = 3"))
                .matches("VALUES (CAST(3 AS BIGINT), CAST(3 AS BIGINT))");
    }

    @Test
    public void testNumberOfLeafPartitionsInUnpartitionedTable() {
        int expectedNoOfLeafPartitionsInUnpartitionedTable = 1; // A root node is a single leaf partition
        assertThat(expectedNoOfLeafPartitionsInUnpartitionedTable).isEqualTo(
                POPULATED_SLEEPER_EXTERNAL_RESOURCE.getStateStore("customer_unpartitioned").getAllPartitions().stream()
                        .filter(Partition::isLeafPartition)
                        .count());
    }

    @Test
    public void testNumberOfLeafPartitionsInPartitionedTable() {
        int expectedNoOfLeafPartitionsInPartitionedTable = (1 + NO_OF_NATION_KEYS / 2) * 2; // There are always an even number of partitions once it has split
        assertThat(expectedNoOfLeafPartitionsInPartitionedTable).isEqualTo(
                POPULATED_SLEEPER_EXTERNAL_RESOURCE.getStateStore("customer_partitioned").getAllPartitions().stream()
                        .filter(Partition::isLeafPartition)
                        .count());
    }

    @Test
    public void testExactlyOneParquetFileInRootPartitionInUnpartitionedTable() {
        StateStore stateStore = POPULATED_SLEEPER_EXTERNAL_RESOURCE.getStateStore("customer_unpartitioned");
        assertThat(stateStore.getPartitionToReferencedFilesMap())
                .containsOnlyKeys("root")
                .extractingByKey("root").satisfies(files -> assertThat(files).hasSize(1));
    }

    @Test
    public void testMaxOneParquetFilePerPartitionInPartitionedTable() {
        StateStore stateStore = POPULATED_SLEEPER_EXTERNAL_RESOURCE.getStateStore("customer_partitioned");
        assertThat(stateStore.getPartitionToReferencedFilesMap())
                .anySatisfy((partitionId, files) -> assertThat(files).hasSize(1));
    }
}
