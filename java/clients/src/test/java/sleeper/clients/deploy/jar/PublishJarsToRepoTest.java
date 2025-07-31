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
import static sleeper.clients.util.command.Command.command;
import static sleeper.clients.util.command.CommandPipeline.pipeline;

public class PublishJarsToRepoTest {
    private final PublishJarsToRepo.Builder genericBuilder = PublishJarsToRepo.builder()
            .pathOfJarsDirectory(Path.of("/some/directory/"))
            .repoUrl("someUrl")
            .version("0.31.0")
            .m2SettingsServerId("repo.id");

    private final ClientJar clientJar1 = ClientJar.builder().filenameFormat("testClient1-%s.jar").artifactId("testClient1Artifact").build();
    private final CommandPipeline clientJarPipeline1 = generateCommandPipeline("testClient1-0.31.0.jar", "testClient1Artifact");
    private final ClientJar clientJar2 = ClientJar.builder().filenameFormat("testClient2-%s.jar").artifactId("testClient2Artifact").build();
    private final CommandPipeline clientJarPipeline2 = generateCommandPipeline("testClient2-0.31.0.jar", "testClient2Artifact");
    private final LambdaJar lambdaJar1 = LambdaJar.builder().filenameFormat("testLambda1-%s.jar").imageName("testLambda1Image").artifactId("testLambda1Artifact").build();
    private final CommandPipeline lambdaJarPipeline1 = generateCommandPipeline("testLambda1-0.31.0.jar", "testLambda1Artifact");
    private final LambdaJar lambdaJar2 = LambdaJar.builder().filenameFormat("testLambda2-%s.jar").imageName("testLambda2Image").artifactId("testLambda2Artifact").build();
    private final CommandPipeline lambdaJarPipeline2 = generateCommandPipeline("testLambda2-0.31.0.jar", "testLambda2Artifact");

    @Test
    public void shouldPublishOneJarWhenJustOneClientJarNoLambda() throws Exception {
        //Given
        List<CommandPipeline> commandsThatRan = new ArrayList<>();
        CommandPipelineRunner commandRunner = recordCommandsRun(commandsThatRan);
        PublishJarsToRepo publishJarsToRepo = genericBuilder
                .commandRunner(commandRunner)
                .clientJars(List.of(clientJar1))
                .lambdaJars(List.of()).build();

        //When
        publishJarsToRepo.upload();

        //Then
        assertThat(commandsThatRan).containsExactly(clientJarPipeline1);
    }

    @Test
    public void shouldPublishOneJarWhenJustOneLambdaJarNoClient() throws Exception {
        //Given
        List<CommandPipeline> commandsThatRan = new ArrayList<>();
        CommandPipelineRunner commandRunner = recordCommandsRun(commandsThatRan);
        PublishJarsToRepo publishJarsToRepo = genericBuilder
                .commandRunner(commandRunner)
                .clientJars(List.of())
                .lambdaJars(List.of(lambdaJar1)).build();

        //When
        publishJarsToRepo.upload();

        //Then
        assertThat(commandsThatRan).containsExactly(lambdaJarPipeline1);
    }

    @Test
    public void shouldPublishAllJarsWhenMultipleClientAndLambda() throws Exception {
        //Given
        List<CommandPipeline> commandsThatRan = new ArrayList<>();
        CommandPipelineRunner commandRunner = recordCommandsRun(commandsThatRan);
        PublishJarsToRepo publishJarsToRepo = genericBuilder
                .commandRunner(commandRunner)
                .clientJars(List.of(clientJar1, clientJar2))
                .lambdaJars(List.of(lambdaJar1, lambdaJar2)).build();

        //When
        publishJarsToRepo.upload();

        //Then
        assertThat(commandsThatRan)
                .containsExactly(clientJarPipeline1, clientJarPipeline2, lambdaJarPipeline1, lambdaJarPipeline2);
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

    private CommandPipeline generateCommandPipeline(String filename, String artifactId) {
        return pipeline(command("mvn", "deploy:deploy-file", "-q",
                "-Durl=someUrl",
                "-DrepositoryId=repo.id",
                "-Dfile=/some/directory/" + filename,
                "-DgroupId=sleeper",
                "-DartifactId=" + artifactId,
                "-Dversion=0.31.0",
                "-DgeneratePom=false"));
    }
}
