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
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.trino.testutils.PopulatedSleeperExternalResource;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class TpchSchemaInsertIT {
    private static final String TPCH_TABLE_SIZE = "tiny";

    private static final List<PopulatedSleeperExternalResource.TableDefinition> TABLE_DEFINITIONS = ImmutableList.of(
            new PopulatedSleeperExternalResource.TableDefinition(
                    "customer",
                    generateCustomerSchema(),
                    List.of(),
                    Stream.empty()),
            new PopulatedSleeperExternalResource.TableDefinition(
                    "orders",
                    generateOrdersSchema(),
                    List.of(),
                    Stream.empty()),
            new PopulatedSleeperExternalResource.TableDefinition(
                    "lineitem",
                    generateLineItemSchema(),
                    List.of(),
                    Stream.empty()));

    @RegisterExtension
    public static final PopulatedSleeperExternalResource POPULATED_SLEEPER_EXTERNAL_RESOURCE = new PopulatedSleeperExternalResource(TABLE_DEFINITIONS);
    private static QueryAssertions assertions;

    @BeforeAll
    public static void init() {
        assertions = POPULATED_SLEEPER_EXTERNAL_RESOURCE.getQueryAssertions();
        assertions.execute(
                String.format("INSERT INTO sleeper.default.customer " +
                        "SELECT name, custkey, address, nationkey, phone, CAST(acctbal AS VARCHAR), mktsegment, comment " +
                        "FROM tpch.%s.customer", TPCH_TABLE_SIZE));
        assertions.execute(
                String.format("INSERT INTO sleeper.default.orders " +
                        "SELECT custkey, orderkey, orderstatus, CAST(totalprice AS VARCHAR), CAST(orderdate AS VARCHAR), " +
                        "orderpriority, clerk, shippriority, comment " +
                        "FROM tpch.%s.orders", TPCH_TABLE_SIZE));
        assertions.execute(
                String.format("INSERT INTO sleeper.default.lineitem " +
                        "SELECT orderkey, partkey, suppkey, linenumber, quantity, CAST(extendedprice AS VARCHAR), " +
                        "CAST(discount AS VARCHAR), CAST(tax AS VARCHAR), returnflag, linestatus, " +
                        "CAST(shipdate AS VARCHAR), CAST(commitdate AS VARCHAR), CAST(receiptdate AS VARCHAR), " +
                        "shipinstruct, shipmode, comment " +
                        "FROM tpch.%s.lineitem", TPCH_TABLE_SIZE));
    }

    private static Schema generateCustomerSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("name", new StringType()))
                .valueFields(
                        new Field("custkey", new LongType()),
                        new Field("address", new StringType()),
                        new Field("nationkey", new LongType()),
                        new Field("phone", new StringType()),
                        new Field("acctbal", new StringType()),
                        new Field("mktsegment", new StringType()),
                        new Field("comment", new StringType()))
                .build();
    }

    private static Schema generateOrdersSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("custkey", new LongType()))
                .valueFields(
                        new Field("orderkey", new LongType()),
                        new Field("orderstatus", new StringType()),
                        new Field("totalprice", new StringType()),
                        new Field("orderdate", new StringType()),
                        new Field("orderpriority", new StringType()),
                        new Field("clerk", new StringType()),
                        new Field("shippriority", new IntType()),
                        new Field("comment", new StringType()))
                .build();
    }

    private static Schema generateLineItemSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("orderkey", new LongType()))
                .valueFields(
                        new Field("partkey", new LongType()),
                        new Field("suppkey", new LongType()),
                        new Field("linenumber", new IntType()),
                        new Field("quantity", new IntType()),
                        new Field("extendedprice", new StringType()),
                        new Field("discount", new StringType()),
                        new Field("tax", new StringType()),
                        new Field("returnflag", new StringType()),
                        new Field("linestatus", new StringType()),
                        new Field("shipdate", new StringType()),
                        new Field("commitdate", new StringType()),
                        new Field("receiptdate", new StringType()),
                        new Field("shipinstruct", new StringType()),
                        new Field("shipmode", new StringType()),
                        new Field("comment", new StringType()))
                .build();
    }

    @Test
    public void testCustomerEq() {
        assertThat(assertions.query(
                "SELECT name, custkey FROM customer WHERE name = 'Customer#000000001'"))
                .matches("VALUES (CAST ('Customer#000000001' AS VARCHAR), CAST(1 AS BIGINT))");
    }

    @Test
    public void testOrdersEq() {
        assertThat(assertions.query(
                "SELECT DISTINCT custkey FROM orders WHERE custkey = 1"))
                .matches("VALUES (CAST(1 AS BIGINT))");
    }

    @Test
    public void testLineItemEq() {
        assertThat(assertions.query(
                "SELECT DISTINCT orderkey FROM lineitem WHERE orderkey = 1"))
                .matches("VALUES (CAST(1 AS BIGINT))");
    }

    @Test
    public void testCustomerCountMinMax() {
        assertThat(assertions.query(
                "SELECT MIN(name), MAX(name), COUNT(*) FROM sleeper.default.customer WHERE name LIKE 'Customer%%'"))
                .matches(String.format(
                        "SELECT CAST(MIN(name) AS VARCHAR), CAST(MAX(name) AS VARCHAR), COUNT(*) " +
                                "FROM tpch.%s.customer WHERE name LIKE 'Customer%%'",
                        TPCH_TABLE_SIZE));
    }

    @Test
    public void testOrdersCountMinMax() {
        assertThat(assertions.query(
                "SELECT MIN(custkey), MAX(custkey), COUNT(*) FROM sleeper.default.orders WHERE custkey BETWEEN 0 AND 999999999999"))
                .matches(String.format(
                        "SELECT MIN(custkey), MAX(custkey), COUNT(*) FROM tpch.%s.orders WHERE custkey BETWEEN 0 AND 999999999999",
                        TPCH_TABLE_SIZE));
    }

    @Test
    public void testLineItemCountMinMax() {
        assertThat(assertions.query(
                "SELECT MIN(orderkey), MAX(orderkey), COUNT(*) FROM sleeper.default.lineitem WHERE orderkey BETWEEN 0 AND 999999999999"))
                .matches(String.format(
                        "SELECT MIN(orderkey), MAX(orderkey), COUNT(*) FROM tpch.%s.lineitem WHERE orderkey BETWEEN 0 AND 999999999999",
                        TPCH_TABLE_SIZE));
    }

    @Test
    public void testCustomerOrdersJoinResults() {
        assertThat(assertions.query("SELECT DISTINCT custkey, name, address " +
                "FROM orders INNER JOIN customer USING (custkey) " +
                "WHERE name = 'Customer#000000001'"))
                .matches("SELECT custkey, name, address FROM customer WHERE name = 'Customer#000000001'");
        assertThat(assertions.query("SELECT custkey, orderkey, totalprice, orderstatus, clerk " +
                "FROM orders INNER JOIN customer USING (custkey) " +
                "WHERE name = 'Customer#000000001'"))
                .matches("SELECT custkey, orderkey, totalprice, orderstatus, clerk FROM orders WHERE custkey = 1");
    }

    //    @Test
    //    It should be possible to test that the dynamic filter is being applied correctly, but it is complicated to
    //    construct the plan to match it against. May be worth revisiting in future.
    //    See TestJoin in the Trino source.
    //    public void testCustomerOrdersJoinPlan() {
    //        assertions.assertQueryAndPlan(
    //                "SELECT custkey, orderkey, totalprice, orderstatus, clerk " +
    //                        "FROM orders INNER JOIN customer USING (custkey) " +
    //                        "WHERE name = 'Customer#000000001'",
    //                "SELECT custkey, orderkey, totalprice, orderstatus, clerk FROM orders WHERE custkey = 1",
    //                // Expected plan goes here // );
    //    }
}
