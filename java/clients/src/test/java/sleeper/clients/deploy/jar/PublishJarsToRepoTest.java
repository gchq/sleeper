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
import sleeper.core.deploy.ClientJar;
import sleeper.core.deploy.LambdaJar;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.clients.testutil.RunCommandTestHelper.recordCommandsRun;

public class PublishJarsToRepoTest {
    private final PublishJarsToRepo.Builder genericBuilder = PublishJarsToRepo.builder()
            .pathOfJarsDirectory(Path.of("/some/directory/"))
            .repoUrl("file:/someRepo")
            .m2SettingsServerId("someId");
    private final List<ClientJar> clientJars = List.of(
            ClientJar.builder().filenameFormat("test1-%s.jar").artifactId("test1Artifact").build(),
            ClientJar.builder().filenameFormat("test2-%s.jar").artifactId("test2Artifact").build());
    private final List<LambdaJar> lambdJars = List.of(
            LambdaJar.builder().filenameFormat("test3-%s.jar").imageName("test3Image").artifactId("test3Artifact").build(),
            LambdaJar.builder().filenameFormat("test4-%s.jar").imageName("test4Image").artifactId("test4Artifact").build());

    @Test
    public void testRunsCommands() throws Exception {
        //Given
        List<CommandPipeline> commandsThatRan = new ArrayList<>();
        CommandPipelineRunner commandRunner = recordCommandsRun(commandsThatRan);
        PublishJarsToRepo publishJarsToRepo = PublishJarsToRepo.builder()
                .pathOfJarsDirectory(Path.of("/some/directory/"))
                .repoUrl("someUrl").version("0.31.0")
                .m2SettingsServerId("repo.id")
                .commandRunner(commandRunner)
                .clientJars(clientJars)
                .lambdaJars(lambdJars).build();

        //When
        publishJarsToRepo.upload();
        List<String> commandsString = commandsThatRan.stream().map(CommandPipeline::toString).toList();

        //Then
        assertThat(commandsString)
                //This one's from ClientJar
                .containsExactly(
                        "[mvn, deploy:deploy-file, -q, -Durl=someUrl, -DrepositoryId=repo.id, " +
                                "-Dfile=/some/directory/test1-0.31.0.jar, -DgroupId=sleeper, " +
                                "-DartifactId=test1Artifact, -Dversion=0.31.0, -DgeneratePom=false]",
                        "[mvn, deploy:deploy-file, -q, -Durl=someUrl, -DrepositoryId=repo.id, " +
                                "-Dfile=/some/directory/test2-0.31.0.jar, -DgroupId=sleeper, " +
                                "-DartifactId=test2Artifact, -Dversion=0.31.0, -DgeneratePom=false]",
                        "[mvn, deploy:deploy-file, -q, -Durl=someUrl, -DrepositoryId=repo.id, " +
                                "-Dfile=/some/directory/test3-0.31.0.jar, -DgroupId=sleeper, " +
                                "-DartifactId=test3Artifact, -Dversion=0.31.0, -DgeneratePom=false]",
                        "[mvn, deploy:deploy-file, -q, -Durl=someUrl, -DrepositoryId=repo.id, " +
                                "-Dfile=/some/directory/test4-0.31.0.jar, -DgroupId=sleeper, " +
                                "-DartifactId=test4Artifact, -Dversion=0.31.0, -DgeneratePom=false]");
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenTooFewArguments() {
        assertThatThrownBy(() -> PublishJarsToRepo.main(new String[]{"anyString", "anyString"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Usage: <jars-dir> <repository url> <m2 settings server id>");
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenTooManyArguments() {
        assertThatThrownBy(() -> PublishJarsToRepo.main(new String[]{"anyString", "anyString", "anyString", "anyString"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Usage: <jars-dir> <repository url> <m2 settings server id>");
    }

    @Test
    public void shouldThrowNullPointerWhenPathOfJarsDirectoryIsNull() {
        assertThatThrownBy(() -> genericBuilder.pathOfJarsDirectory(null).build())
                .isInstanceOf(NullPointerException.class)
                .hasMessage("File path must not be null");
    }

    @Test
    public void shouldThrowNullPointerWhenRepositoryUrlIsNull() {
        assertThatThrownBy(() -> genericBuilder.repoUrl(null).build())
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Repository URL must not be null");
    }

    @Test
    public void shouldThrowNullPointerWhenm2SettingsServerIdIsNull() {
        assertThatThrownBy(() -> genericBuilder.m2SettingsServerId(null).build())
                .isInstanceOf(NullPointerException.class)
                .hasMessage("M2 settings server Id must not be null");
    }

    @Test
    public void shouldThrowNullPointerWhenVerisonIsNull() {
        assertThatThrownBy(() -> genericBuilder.version(null).build())
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Version to publish must not be null");
    }

    @Test
    public void shouldThrowNullPointerWhenCommandRunnerIsNull() {
        assertThatThrownBy(() -> genericBuilder.commandRunner(null).build())
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Command Runner must not be null");
    }

    @Test
    public void shouldThrowNullPointerWhenClientJarsIsNull() {
        assertThatThrownBy(() -> genericBuilder.clientJars(null).build())
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Client jars must not be null");
    }

    @Test
    public void shouldThrowNullPointerWhenLambdaJarsIsNull() {
        assertThatThrownBy(() -> genericBuilder.lambdaJars(null).build())
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Lambda jars must not be null");
    }
}
