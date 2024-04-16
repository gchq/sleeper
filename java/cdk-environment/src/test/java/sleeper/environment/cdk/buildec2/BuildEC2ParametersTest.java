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

public class BuildEC2ParametersTest {

    @Test
    public void fillGitClone() {
        assertThat(BuildEC2Parameters.from(AppContext.of(
                BRANCH.value("feature/test"), FORK.value("test-fork"), REPOSITORY.value("test-project")))
                .fillUserDataTemplate("git clone -b ${branch} https://github.com/${fork}/${repository}.git"))
                .isEqualTo("git clone -b feature/test https://github.com/test-fork/test-project.git");
    }

    @Test
    public void fillLoginUser() {
        assertThat(BuildEC2Parameters.from(AppContext.of(LOGIN_USER.value("test-user")))
                .fillUserDataTemplate("LOGIN_USER=${loginUser}\n" +
                        "LOGIN_HOME=/home/$LOGIN_USER"))
                .isEqualTo("LOGIN_USER=test-user\n" +
                        "LOGIN_HOME=/home/$LOGIN_USER");
    }

    @Test
    public void templateCanContainSameKeyMultipleTimes() {
        assertThat(BuildEC2Parameters.from(AppContext.of(REPOSITORY.value("repeated-repo")))
                .fillUserDataTemplate("[ ! -d ~/${repository} ] && mkdir ~/${repository}"))
                .isEqualTo("[ ! -d ~/repeated-repo ] && mkdir ~/repeated-repo");
    }

    @Test
    public void setDefaultParametersWhenUsingEmptyContext() {
        assertThat(BuildEC2Parameters.from(AppContext.empty()))
                .usingRecursiveComparison()
                .isEqualTo(BuildEC2Parameters.from(AppContext.of(
                        REPOSITORY.value("sleeper"), FORK.value("gchq"), BRANCH.value("main"))));
    }

}
