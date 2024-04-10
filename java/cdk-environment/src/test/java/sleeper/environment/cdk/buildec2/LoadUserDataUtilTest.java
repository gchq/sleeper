/*
 * Copyright 2022-2024 Crown Copyright
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

import org.junit.jupiter.api.Test;

import sleeper.environment.cdk.config.AppContext;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.environment.cdk.buildec2.BuildEC2Image.LOGIN_USER;
import static sleeper.environment.cdk.buildec2.BuildEC2Parameters.BRANCH;
import static sleeper.environment.cdk.buildec2.BuildEC2Parameters.FORK;
import static sleeper.environment.cdk.buildec2.BuildEC2Parameters.REPOSITORY;

class LoadUserDataUtilTest {

    @Test
    void canLoadUserData() {
        String roleName = "test-role-for-ec2";
        assertThat(LoadUserDataUtil.userData(roleName,
                BuildEC2Parameters.from(AppContext.of(
                        LOGIN_USER.value("test-user"),
                        REPOSITORY.value("a-repo"),
                        FORK.value("a-fork"),
                        BRANCH.value("feature/something")))))
                .startsWith("Content-Type: multipart/mixed;")
                .contains("LOGIN_USER=test-user" + System.lineSeparator() +
                        "REPOSITORY=a-repo" + System.lineSeparator() +
                        "FORK=a-fork" + System.lineSeparator() +
                        "BRANCH=feature/something" + System.lineSeparator() +
                        "ROLE=test-role-for-ec2" + System.lineSeparator());
    }

}
