/*
 * Copyright 2022 Crown Copyright
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
package sleeper.environment.cdk.buildec2;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class LoadUserDataUtilTest {

    @Test
    public void can_load_user_data() {
        assertThat(LoadUserDataUtil.base64(BuildEC2ParamsTest.params().build()))
                .startsWith("Q29udGVudC1UeXBlOiBtdWx0aXBhcnQvbWl4ZWQ7IGJvdW5kYXJ5PSIvLyIKTUlNR");
    }

    @Test
    public void can_fill_parameters() {
        assertThat(LoadUserDataUtil.userData(BuildEC2Params.builder()
                .branch("feature/something").fork("a-fork").repository("a-repo").build()))
                .contains("git clone -b feature/something https://github.com/a-fork/a-repo.git");
    }
}
