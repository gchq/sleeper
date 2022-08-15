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

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;
import sleeper.environment.cdk.config.AppContext;
import sleeper.environment.cdk.config.StringValue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.environment.cdk.buildec2.BuildEC2Parameters.*;
import static sleeper.environment.cdk.config.AppParameters.BUILD_REPOSITORY;

public class BuildEC2ParametersTest {

    public static AppContext context(StringValue... overrides) {
        return AppContext.of(ArrayUtils.addAll(overrides,
                REPOSITORY.value("test-project"),
                FORK.value("test-fork"),
                BRANCH.value("feature/test")));
    }

    @Test
    public void canFillTemplate() {
        assertThat(BuildEC2Parameters.from(context()).fillUserDataTemplate("git clone -b ${branch} https://github.com/${fork}/${repository}.git"))
                .isEqualTo("git clone -b feature/test https://github.com/test-fork/test-project.git");
    }

    @Test
    public void templateCanContainSameKeyMultipleTimes() {
        assertThat(BuildEC2Parameters.from(context(REPOSITORY.value("repeated-repo")))
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

    @Test
    public void refuseEmptyString() {
        AppContext context = context(BUILD_REPOSITORY.value(""));
        assertThatThrownBy(() -> BuildEC2Parameters.from(context))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("repository");
    }
}
