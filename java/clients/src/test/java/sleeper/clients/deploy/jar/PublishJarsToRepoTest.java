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

import sleeper.clients.util.command.CommandPipeline;
import sleeper.clients.util.command.CommandPipelineRunner;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.clients.testutil.RunCommandTestHelper.recordCommandsRun;

public class PublishJarsToRepoTest {

    @Test
    public void testRunsCommands() throws Exception {
        //Given
        List<CommandPipeline> commandsThatRan = new ArrayList<>();
        CommandPipelineRunner commandRunner = recordCommandsRun(commandsThatRan);
        PublishJarsToRepo publishJarsToRepo = PublishJarsToRepo.builder().pathOfJarsDirectory(Path.of("/some/directory/")).repoUrl("someUrl").version("0.31.0").commandRunner(commandRunner).build();

        //When
        publishJarsToRepo.upload();
        List<String> commandsString = commandsThatRan.stream().map(CommandPipeline::toString).toList();

        //Then
        assertThat(commandsString)
                //This one's from ClientJar
                .contains("[mvn, deploy:deploy-file, -q, -Durl=someUrl, -DrepositoryId=repo.id, " +
                        "-Dfile=/some/directory/bulk-import-runner-0.31.0.jar, -DgroupId=sleeper, " +
                        "-DartifactId=bulk-import-runner, -Dversion=0.31.0, -DgeneratePom=false]")
                //This one's from LambdaJar
                .contains("[mvn, deploy:deploy-file, -q, -Durl=someUrl, -DrepositoryId=repo.id, " +
                        "-Dfile=/some/directory/athena-0.31.0.jar, -DgroupId=sleeper, " +
                        "-DartifactId=athena, -Dversion=0.31.0, -DgeneratePom=false]");
    }

    @Test
    public void shouldThrowNullPointerWhenPathOfJarsDirectoryIsNull() {
        assertThatThrownBy(() -> PublishJarsToRepo.builder().pathOfJarsDirectory(null).repoUrl("file:/someRepo").build())
                .isInstanceOf(NullPointerException.class)
                .hasMessage("File path must not be null");
    }

    @Test
    public void shouldThrowNullPointerWhenRepositoryUrlIsNull() {
        assertThatThrownBy(() -> PublishJarsToRepo.builder().repoUrl(null).pathOfJarsDirectory(Path.of("any")).build())
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Repository URL must not be null");
    }

    @Test
    public void shouldThrowNullPointerWhenVerisonIsNull() {
        assertThatThrownBy(() -> PublishJarsToRepo.builder().pathOfJarsDirectory(Path.of("any")).repoUrl("file:/someRepo").version(null).build())
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Version to publish must not be null");
    }

    @Test
    public void shouldThrowNullPointerWhenCommandRunnerISNull() {
        assertThatThrownBy(() -> PublishJarsToRepo.builder().pathOfJarsDirectory(Path.of("any")).repoUrl("file:/someRepo").commandRunner(null).build())
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Command Runner must not be null");
    }
}
