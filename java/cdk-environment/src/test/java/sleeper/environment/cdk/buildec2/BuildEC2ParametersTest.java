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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class BuildEC2ParametersTest {

    public static BuildEC2Parameters.Builder params() {
        return BuildEC2Parameters.builder()
                .repository("test-project")
                .fork("test-fork")
                .branch("feature/test");
    }

    @Test
    public void canFillTemplate() {
        assertThat(params().build().fillUserDataTemplate("git clone -b ${branch} https://github.com/${fork}/${repository}.git"))
                .isEqualTo("git clone -b feature/test https://github.com/test-fork/test-project.git");
    }

    @Test
    public void templateCanContainSameKeyMultipleTimes() {
        assertThat(params().repository("repeated-repo").build()
                .fillUserDataTemplate("[ ! -d ~/${repository} ] && mkdir ~/${repository}"))
                .isEqualTo("[ ! -d ~/repeated-repo ] && mkdir ~/repeated-repo");
    }

    @Test
    public void refuseEmptyString() {
        assertThatThrownBy(params().repository("")::build)
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("repository");
    }

    @Test
    public void canBuildParamsFromAContextFunction() {
        Map<String, Object> context = new HashMap<>();
        context.put("repository", "some-repo");
        context.put("fork", "some-fork");
        context.put("branch", "some-branch");

        assertThat(BuildEC2Parameters.from(context::get))
                .usingRecursiveComparison()
                .isEqualTo(BuildEC2Parameters.builder()
                        .repository("some-repo")
                        .fork("some-fork")
                        .branch("some-branch")
                        .build());
    }

    @Test
    public void setDefaultParametersWhenUsingEmptyContext() {
        assertThat(BuildEC2Parameters.from(Collections.emptyMap()::get))
                .usingRecursiveComparison()
                .isEqualTo(BuildEC2Parameters.builder()
                        .repository("sleeper")
                        .fork("gchq")
                        .branch("main")
                        .build());
    }
}
