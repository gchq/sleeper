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
import static sleeper.environment.cdk.buildec2.BuildEC2Image.LOGIN_USER;
import static sleeper.environment.cdk.buildec2.BuildEC2Parameters.BRANCH;
import static sleeper.environment.cdk.buildec2.BuildEC2Parameters.FORK;
import static sleeper.environment.cdk.buildec2.BuildEC2Parameters.REPOSITORY;
import static sleeper.environment.cdk.config.AppParameters.NIGHTLY_TEST_BUCKET;
import static sleeper.environment.cdk.config.AppParameters.NIGHTLY_TEST_DEPLOY_ID;
import static sleeper.environment.cdk.config.AppParameters.NIGHTLY_TEST_RUN_ENABLED;
import static sleeper.environment.cdk.config.AppParameters.NIGHTLY_TEST_RUN_HOUR_UTC;
import static sleeper.environment.cdk.config.AppParameters.NIGHTLY_TEST_SUBNETS;
import static sleeper.environment.cdk.config.AppParameters.VPC_ID;

class LoadUserDataUtilTest {

    @Test
    void shouldLoadUserDataWithNoNightlyTests() {
        assertThat(LoadUserDataUtil.userData(BuildEC2Parameters.from(AppContext.of(
                LOGIN_USER.value("test-user"),
                REPOSITORY.value("a-repo"),
                FORK.value("a-fork"),
                BRANCH.value("feature/something")))))
                .startsWith("Content-Type: multipart/mixed;")
                .contains("LOGIN_USER=test-user" + System.lineSeparator() +
                        "REPOSITORY=a-repo" + System.lineSeparator() +
                        "FORK=a-fork" + System.lineSeparator() +
                        "BRANCH=feature/something" + System.lineSeparator())
                .doesNotContain("write_files");
    }

    @Test
    void shouldLoadUserDataWithNightlyTests() {
        assertThat(LoadUserDataUtil.userData(BuildEC2Parameters.from(AppContext.of(
                LOGIN_USER.value("test-user"),
                REPOSITORY.value("a-repo"),
                FORK.value("a-fork"),
                BRANCH.value("feature/something"),
                NIGHTLY_TEST_RUN_ENABLED.value(true),
                NIGHTLY_TEST_DEPLOY_ID.value("mt"),
                VPC_ID.value("my-vpc"),
                NIGHTLY_TEST_SUBNETS.value("subnet-1", "subnet-2"),
                NIGHTLY_TEST_BUCKET.value("my-bucket")))))
                .startsWith("Content-Type: multipart/mixed;")
                .contains("LOGIN_USER=test-user" + System.lineSeparator() +
                        "REPOSITORY=a-repo" + System.lineSeparator() +
                        "FORK=a-fork" + System.lineSeparator() +
                        "BRANCH=feature/something" + System.lineSeparator())
                .contains("write_files");
    }

    @Test
    void shouldLoadNightlyTestSettings() {
        assertThat(LoadUserDataUtil.nightlyTestSettingsJson(BuildEC2Parameters.from(AppContext.of(
                NIGHTLY_TEST_RUN_ENABLED.value(true),
                NIGHTLY_TEST_DEPLOY_ID.value("mt"),
                VPC_ID.value("my-vpc"),
                NIGHTLY_TEST_SUBNETS.value("subnet-1", "subnet-2"),
                NIGHTLY_TEST_BUCKET.value("my-bucket"),
                FORK.value("my-fork"),
                REPOSITORY.value("my-repo")))))
                .contains("\"deployId\": \"mt\"")
                .contains("\"vpc\": \"my-vpc\"")
                .contains("\"subnets\": \"subnet-1,subnet-2\"")
                .contains("\"repoPath\": \"my-fork/my-repo\"")
                .contains("\"resultsBucket\": \"my-bucket\"");
    }

    @Test
    void shouldLoadCrontab() {
        assertThat(LoadUserDataUtil.crontab(BuildEC2Parameters.from(AppContext.of(
                NIGHTLY_TEST_RUN_ENABLED.value(true),
                NIGHTLY_TEST_DEPLOY_ID.value("mt"),
                NIGHTLY_TEST_RUN_HOUR_UTC.value(3),
                LOGIN_USER.value("my-user"),
                VPC_ID.value("my-vpc"),
                NIGHTLY_TEST_SUBNETS.value("subnet-1", "subnet-2"),
                NIGHTLY_TEST_BUCKET.value("my-bucket")))))
                .contains("PATH=$PATH:/usr/bin:/home/my-user/.local/bin")
                .contains("0 3 * * TUE,THU,SAT,SUN");
    }

}
