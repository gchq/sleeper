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
package sleeper.clients.deploy.jar;

import org.junit.jupiter.api.Test;

import sleeper.core.SleeperVersion;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PublishJarsToRepoTest {

    @Test
    public void testRunsCommands() throws Exception {
        /*
         * List<CommandPipeline> commandsThatRan = pipelinesRunOn(
         * runCommand -> {
         * PublishJarsToRepo.builder().repoUrl("file:/someRepo").version(SleeperVersion.getVersion().commandRunner(
         * runCommand).build().upload();
         * });
         */

        //assertThat(commandsThatRan).contains(pipeline(command("java", "--version")));
    }

    @Test
    public void testRepoMustNotBeNull() {
        assertThatThrownBy(() -> PublishJarsToRepo.builder().version(SleeperVersion.getVersion()).build())
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Repository URL must not be null");
    }

    @Test
    public void testVersionMustNotBeNull() {
        assertThatThrownBy(() -> PublishJarsToRepo.builder().repoUrl("file:/someRepo").version(null).build())
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Version to publish must not be null");
    }

    @Test
    public void testCommandRunnerMustNotBeNull() {
        assertThatThrownBy(() -> PublishJarsToRepo.builder().repoUrl("file:/someRepo").commandRunner(null).build())
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Command Runner must not be null");
    }
}
