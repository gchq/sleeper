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
package sleeper.environment.cdk.buildec2;

import org.junit.jupiter.api.Test;

import sleeper.environment.cdk.config.AppContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.environment.cdk.buildec2.BuildEC2Image.LOGIN_USER;
import static sleeper.environment.cdk.buildec2.BuildEC2Parameters.BRANCH;
import static sleeper.environment.cdk.buildec2.BuildEC2Parameters.FORK;
import static sleeper.environment.cdk.buildec2.BuildEC2Parameters.REPOSITORY;
import static sleeper.environment.cdk.config.AppParameters.NIGHTLY_TEST_DEPLOY_ID;
import static sleeper.environment.cdk.config.AppParameters.NIGHTLY_TEST_RUN_ENABLED;
import static sleeper.environment.cdk.config.AppParameters.NIGHTLY_TEST_SUBNETS;
import static sleeper.environment.cdk.config.AppParameters.VPC_ID;

public class BuildEC2ParametersTest {

    @Test
    void shouldFillGitClone() {
        assertThat(BuildEC2Parameters.from(AppContext.of(
                BRANCH.value("feature/test"),
                FORK.value("test-fork"),
                REPOSITORY.value("test-project")))
                .fillUserDataTemplate("git clone -b ${branch} https://github.com/${fork}/${repository}.git"))
                .isEqualTo("git clone -b feature/test https://github.com/test-fork/test-project.git");
    }

    @Test
    void shouldFillLoginUser() {
        assertThat(BuildEC2Parameters.from(AppContext.of(
                LOGIN_USER.value("test-user")))
                .fillUserDataTemplate("LOGIN_USER=${loginUser}\n" +
                        "LOGIN_HOME=/home/$LOGIN_USER"))
                .isEqualTo("LOGIN_USER=test-user\n" +
                        "LOGIN_HOME=/home/$LOGIN_USER");
    }

    @Test
    void templateCanContainSameKeyMultipleTimes() {
        assertThat(BuildEC2Parameters.from(AppContext.of(
                REPOSITORY.value("repeated-repo")))
                .fillUserDataTemplate("[ ! -d ~/${repository} ] && mkdir ~/${repository}"))
                .isEqualTo("[ ! -d ~/repeated-repo ] && mkdir ~/repeated-repo");
    }

    @Test
    void shouldSetDefaultParametersWhenUsingEmptyContext() {
        assertThat(BuildEC2Parameters.from(AppContext.empty()))
                .usingRecursiveComparison()
                .isEqualTo(BuildEC2Parameters.from(AppContext.of(
                        REPOSITORY.value("sleeper"),
                        FORK.value("gchq"),
                        BRANCH.value("develop"))));
    }

    @Test
    void shouldFillNightlyTestSettings() {
        assertThat(BuildEC2Parameters.builder()
                .context(AppContext.of(
                        NIGHTLY_TEST_RUN_ENABLED.value(true),
                        NIGHTLY_TEST_DEPLOY_ID.value("mt"),
                        VPC_ID.value("my-vpc"),
                        NIGHTLY_TEST_SUBNETS.value("subnet-1,subnet-2"),
                        FORK.value("my-fork"),
                        REPOSITORY.value("my-repo")))
                .testBucket("nightly-test-results")
                .build().fillUserDataTemplate("{" +
                        "\"deployId\": \"${deployId}\"," +
                        "\"vpc\":\"${vpc}\"," +
                        "\"subnets\":\"${subnets}\"," +
                        "\"resultsBucket\":\"${testBucket}\"," +
                        "\"repoPath\":\"${fork}/${repository}\"}"))
                .isEqualTo("{" +
                        "\"deployId\": \"mt\"," +
                        "\"vpc\":\"my-vpc\"," +
                        "\"subnets\":\"subnet-1,subnet-2\"," +
                        "\"resultsBucket\":\"nightly-test-results\"," +
                        "\"repoPath\":\"my-fork/my-repo\"}");
    }

    @Test
    void shouldFillNoNightlyTestSettings() {
        assertThat(BuildEC2Parameters.builder()
                .context(AppContext.empty())
                .testBucket(null)
                .build().fillUserDataTemplate("{" +
                        "\"deployId\": \"${deployId}\"," +
                        "\"vpc\":\"${vpc}\"," +
                        "\"subnets\":\"${subnets}\"," +
                        "\"resultsBucket\":\"${testBucket}\"," +
                        "\"repoPath\":\"${fork}/${repository}\"}"))
                .isEqualTo("{" +
                        "\"deployId\": \"${deployId}\"," +
                        "\"vpc\":\"${vpc}\"," +
                        "\"subnets\":\"${subnets}\"," +
                        "\"resultsBucket\":\"${testBucket}\"," +
                        "\"repoPath\":\"gchq/sleeper\"}");
    }

    @Test
    void shouldRefuseTooLongNightlyTestDeployId() {
        AppContext context = AppContext.of(
                NIGHTLY_TEST_RUN_ENABLED.value(true),
                NIGHTLY_TEST_DEPLOY_ID.value("abc"),
                VPC_ID.value("my-vpc"),
                NIGHTLY_TEST_SUBNETS.value("subnet-1,subnet-2"),
                FORK.value("my-fork"),
                REPOSITORY.value("my-repo"));

        assertThatThrownBy(() -> BuildEC2Parameters.from(context))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("nightlyTestDeployId must be at most 2 characters long");
    }

    @Test
    void shouldRefuseNoNightlyTestDeployId() {
        AppContext context = AppContext.of(
                NIGHTLY_TEST_RUN_ENABLED.value(true),
                VPC_ID.value("my-vpc"),
                NIGHTLY_TEST_SUBNETS.value("subnet-1,subnet-2"),
                FORK.value("my-fork"),
                REPOSITORY.value("my-repo"));

        assertThatThrownBy(() -> BuildEC2Parameters.from(context))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("nightlyTestDeployId must be set (up to 2 characters)");
    }
}
