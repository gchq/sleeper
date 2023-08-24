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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.suite.testutil.PartitionsTestHelper.create128SplitPoints;

public class PartitionsTestHelperTest {

    @Test
    void shouldGenerate128SplitPoints() {
        assertThat(create128SplitPoints()).containsExactly(
                "aa", "af", "ak", "ap", "au",
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
}
