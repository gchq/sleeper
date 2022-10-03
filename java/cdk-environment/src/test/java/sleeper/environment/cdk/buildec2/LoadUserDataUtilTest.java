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
import sleeper.environment.cdk.config.AppContext;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.environment.cdk.buildec2.BuildEC2Parameters.BRANCH;
import static sleeper.environment.cdk.buildec2.BuildEC2Parameters.FORK;
import static sleeper.environment.cdk.buildec2.BuildEC2Parameters.REPOSITORY;

public class LoadUserDataUtilTest {

    @Test
    public void canLoadUserData() {
        assertThat(LoadUserDataUtil.userData(BuildEC2Parameters.from(AppContext.of(
                BRANCH.value("feature/something"), FORK.value("a-fork"), REPOSITORY.value("a-repo")))))
                .startsWith("Content-Type: multipart/mixed;")
                .contains("git clone -b feature/something https://github.com/a-fork/a-repo.git");
    }

}
