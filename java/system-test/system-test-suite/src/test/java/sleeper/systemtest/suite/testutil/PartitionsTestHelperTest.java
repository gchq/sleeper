/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.systemtest.suite.testutil;

import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.junit.jupiter.api.Test;

import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;
import sleeper.systemtest.datageneration.RandomRecordSupplier;
import sleeper.systemtest.datageneration.RandomRecordSupplierConfig;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.suite.testutil.PartitionsTestHelper.create127SplitPoints;
import static sleeper.systemtest.suite.testutil.PartitionsTestHelper.create128Partitions;

public class PartitionsTestHelperTest {

    @Test
    void shouldGenerate128Partitions() {
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        PartitionTree tree = create128Partitions(schema);

        assertThat(tree.traverseLeavesFirst().takeWhile(Partition::isLeafPartition))
                .hasSize(128);
    }

    @Test
    void shouldFill128PartitionsWithData() {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        List<Key> keys = generateNRandomKeys(schema, 10000);
        PartitionTree tree = create128Partitions(schema);
        List<Partition> allLeafPartitions = tree.traverseLeavesFirst()
                .takeWhile(Partition::isLeafPartition)
                .collect(Collectors.toUnmodifiableList());

        // When
        Set<Partition> partitionsWithData = keys.stream()
                .map(tree::getLeafPartition)
                .collect(Collectors.toSet());

        // Then
        assertThat(partitionsWithData).containsExactlyInAnyOrderElementsOf(allLeafPartitions);
    }

    @Test
    void shouldGenerate127SplitPoints() {
        assertThat(create127SplitPoints()).containsExactly(
                "af", "ak", "ap", "au",
                "ba", "bf", "bk", "bp", "bu",
                "ca", "cf", "ck", "cp", "cu",
                "da", "df", "dk", "dp", "du",
                "ea", "ef", "ek", "ep", "eu",
                "fa", "ff", "fk", "fp", "fu",
                "ga", "gf", "gk", "gp", "gu",
                "ha", "hf", "hk", "hp", "hu",
                "ia", "if", "ik", "ip", "iu",
                "ja", "jf", "jk", "jp", "ju",
                "ka", "kf", "kk", "kp", "ku",
                "la", "lf", "lk", "lp", "lu",
                "ma", "mf", "mk", "mp", "mu",
                "na", "nf", "nk", "np", "nu",
                "oa", "of", "ok", "op", "ou",
                "pa", "pf", "pk", "pp", "pu",
                "qa", "qf", "qk", "qp", "qu",
                "ra", "rf", "rk", "rp", "ru",
                "sa", "sf", "sk", "sp", "su",
                "ta", "tf", "tk", "tp", "tu",
                "ua", "uf", "uk", "up", "uu",
                "va", "vf", "vk", "vp", "vu",
                "wa", "wf", "wk", "wp", "wu",
                "xa", "xf", "xk", "xp", "xu",
                "ya", "yf", "yk", "yp", "yu",
                "za", "zf", "zk");
    }

    private static List<Key> generateNRandomKeys(Schema schema, int n) {
        SystemTestStandaloneProperties properties = new SystemTestStandaloneProperties();
        RandomGenerator generator = new JDKRandomGenerator();
        generator.setSeed(0);
        RandomRecordSupplierConfig config = new RandomRecordSupplierConfig(properties, generator);
        RandomRecordSupplier supplier = new RandomRecordSupplier(schema, config);
        return LongStream.range(0, n).mapToObj(i -> supplier.get())
                .map(record -> Key.create(record.getValues(schema.getRowKeyFieldNames())))
                .collect(Collectors.toUnmodifiableList());
    }
}
