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
import static sleeper.systemtest.suite.testutil.PartitionsTestHelper.create127StringSplitPoints;
import static sleeper.systemtest.suite.testutil.PartitionsTestHelper.create128StringPartitions;
import static sleeper.systemtest.suite.testutil.PartitionsTestHelper.create511StringSplitPoints;

public class PartitionsTestHelperTest {

    @Test
    void shouldGenerate128Partitions() {
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        PartitionTree tree = create128StringPartitions(schema);

        assertThat(tree.traverseLeavesFirst().takeWhile(Partition::isLeafPartition))
                .hasSize(128);
    }

    @Test
    void shouldFill128PartitionsWithData() {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        List<Key> keys = generateNRandomKeys(schema, 10000);
        PartitionTree tree = create128StringPartitions(schema);
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
        assertThat(create127StringSplitPoints()).containsExactly(
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

    @Test
    void shouldGenerate511SplitPoints() {
        assertThat(create511StringSplitPoints()).containsExactly(
                "ab", "ac", "ad", "af", "ag", "ah", "aj", "ak", "al", "an", "ao", "ap", "aq", "as", "at", "au", "aw", "ax", "ay",
                "ba", "bb", "bc", "bd", "bf", "bg", "bh", "bj", "bk", "bl", "bn", "bo", "bp", "bq", "bs", "bt", "bu", "bw", "bx", "by",
                "ca", "cb", "cc", "cd", "cf", "cg", "ch", "cj", "ck", "cl", "cn", "co", "cp", "cq", "cs", "ct", "cu", "cw", "cx", "cy",
                "da", "db", "dc", "dd", "df", "dg", "dh", "dj", "dk", "dl", "dn", "do", "dp", "dq", "ds", "dt", "du", "dw", "dx", "dy",
                "ea", "eb", "ec", "ed", "ef", "eg", "eh", "ej", "ek", "el", "en", "eo", "ep", "eq", "es", "et", "eu", "ew", "ex", "ey",
                "fa", "fb", "fc", "fd", "ff", "fg", "fh", "fj", "fk", "fl", "fn", "fo", "fp", "fq", "fs", "ft", "fu", "fw", "fx", "fy",
                "ga", "gb", "gc", "gd", "gf", "gg", "gh", "gj", "gk", "gl", "gn", "go", "gp", "gq", "gs", "gt", "gu", "gw", "gx", "gy",
                "ha", "hb", "hc", "hd", "hf", "hg", "hh", "hj", "hk", "hl", "hn", "ho", "hp", "hq", "hs", "ht", "hu", "hw", "hx", "hy",
                "ia", "ib", "ic", "id", "if", "ig", "ih", "ij", "ik", "il", "in", "io", "ip", "iq", "is", "it", "iu", "iw", "ix", "iy",
                "ja", "jb", "jc", "jd", "jf", "jg", "jh", "jj", "jk", "jl", "jn", "jo", "jp", "jq", "js", "jt", "ju", "jw", "jx", "jy",
                "ka", "kb", "kc", "kd", "kf", "kg", "kh", "kj", "kk", "kl", "kn", "ko", "kp", "kq", "ks", "kt", "ku", "kw", "kx", "ky",
                "la", "lb", "lc", "ld", "lf", "lg", "lh", "lj", "lk", "ll", "ln", "lo", "lp", "lq", "ls", "lt", "lu", "lw", "lx", "ly",
                "ma", "mb", "mc", "md", "mf", "mg", "mh", "mj", "mk", "ml", "mn", "mo", "mp", "mq", "ms", "mt", "mu", "mw", "mx", "my",
                "na", "nb", "nc", "nd", "nf", "ng", "nh", "nj", "nk", "nl", "nn", "no", "np", "nq", "ns", "nt", "nu", "nw", "nx", "ny",
                "oa", "ob", "oc", "od", "of", "og", "oh", "oj", "ok", "ol", "on", "oo", "op", "oq", "os", "ot", "ou", "ow", "ox", "oy",
                "pa", "pb", "pc", "pd", "pf", "pg", "ph", "pj", "pk", "pl", "pn", "po", "pp", "pq", "ps", "pt", "pu", "pw", "px", "py",
                "qa", "qb", "qc", "qd", "qf", "qg", "qh", "qj", "qk", "ql", "qn", "qo", "qp", "qq", "qs", "qt", "qu", "qw", "qx", "qy",
                "ra", "rb", "rc", "rd", "rf", "rg", "rh", "rj", "rk", "rl", "rn", "ro", "rp", "rq", "rs", "rt", "ru", "rw", "rx", "ry",
                "sa", "sb", "sc", "sd", "sf", "sg", "sh", "sj", "sk", "sl", "sn", "so", "sp", "sq", "ss", "st", "su", "sw", "sx", "sy",
                "ta", "tb", "tc", "td", "tf", "tg", "th", "tj", "tk", "tl", "tn", "to", "tp", "tq", "ts", "tt", "tu", "tw", "tx", "ty",
                "ua", "ub", "uc", "ud", "uf", "ug", "uh", "uj", "uk", "ul", "un", "uo", "up", "uq", "us", "ut", "uu", "uw", "ux", "uy",
                "va", "vb", "vc", "vd", "vf", "vg", "vh", "vj", "vk", "vl", "vn", "vo", "vp", "vq", "vs", "vt", "vu", "vw", "vx", "vy",
                "wa", "wb", "wc", "wd", "wf", "wg", "wh", "wj", "wk", "wl", "wn", "wo", "wp", "wq", "ws", "wt", "wu", "ww", "wx", "wy",
                "xa", "xb", "xc", "xd", "xf", "xg", "xh", "xj", "xk", "xl", "xn", "xo", "xp", "xq", "xs", "xt", "xu", "xw", "xx", "xy",
                "ya", "yb", "yc", "yd", "yf", "yg", "yh", "yj", "yk", "yl", "yn", "yo", "yp", "yq", "ys", "yt", "yu", "yw", "yx", "yy",
                "za", "zb", "zc", "zd", "zf", "zg", "zh", "zj", "zk", "zl", "zn", "zo");
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
