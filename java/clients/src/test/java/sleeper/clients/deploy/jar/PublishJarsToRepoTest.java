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

import sleeper.clients.util.command.CommandFailedException;
import sleeper.clients.util.command.CommandPipeline;
import sleeper.clients.util.command.CommandPipelineRunner;
import sleeper.core.deploy.ClientJar;
import sleeper.core.deploy.LambdaJar;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.clients.testutil.RunCommandTestHelper.recordCommandsRun;
import static sleeper.clients.testutil.RunCommandTestHelper.returnExitCode;
import static sleeper.clients.util.command.Command.command;
import static sleeper.clients.util.command.CommandPipeline.pipeline;

public class PublishJarsToRepoTest {
    private final List<CommandPipeline> commandsThatRan = new ArrayList<>();
    private final CommandPipelineRunner commandRunner = recordCommandsRun(commandsThatRan);
    private final PublishJarsToRepo.Builder genericBuilder = generateGenericBuilder();

    private final ClientJar clientJar1 = ClientJar.builder()
            .filenameFormat("testClient1-%s.jar")
            .artifactId("testClient1Artifact")
            .build();
    private final CommandPipeline clientJarPipeline1 = generateMavenDeployFileCommand("testClient1-0.31.0.jar", "testClient1Artifact");
    private final LambdaJar lambdaJar1 = LambdaJar.builder()
            .filenameFormat("testLambda1-%s.jar")
            .imageName("testLambda1Image")
            .artifactId("testLambda1Artifact")
            .build();
    private final CommandPipeline lambdaJarPipeline1 = generateMavenDeployFileCommand("testLambda1-0.31.0.jar", "testLambda1Artifact");

    @Test
    public void shouldPublishOneJarWhenJustOneClientJarNoLambda() throws Exception {
        //Given
        PublishJarsToRepo publishJarsToRepo = genericBuilder
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
        PublishJarsToRepo publishJarsToRepo = genericBuilder
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
        ClientJar clientJar2 = ClientJar.builder()
                .filenameFormat("testClient2-%s.jar")
                .artifactId("testClient2Artifact")
                .build();
        CommandPipeline clientJarPipeline2 = generateMavenDeployFileCommand("testClient2-0.31.0.jar", "testClient2Artifact");
        LambdaJar lambdaJar2 = LambdaJar.builder()
                .filenameFormat("testLambda2-%s.jar")
                .imageName("testLambda2Image")
                .artifactId("testLambda2Artifact")
                .build();
        CommandPipeline lambdaJarPipeline2 = generateMavenDeployFileCommand("testLambda2-0.31.0.jar", "testLambda2Artifact");

        PublishJarsToRepo publishJarsToRepo = genericBuilder
                .clientJars(List.of(clientJar1, clientJar2))
                .lambdaJars(List.of(lambdaJar1, lambdaJar2))
                .build();

        //When
        publishJarsToRepo.upload();

        //Then
        assertThat(commandsThatRan)
                .containsExactly(clientJarPipeline1, clientJarPipeline2, lambdaJarPipeline1, lambdaJarPipeline2);
    }

    @Test
    public void shouldThrowExceptionOutWhenRunningMavenCommandReturnsNonZero() throws IOException, InterruptedException {
        //Given
        CommandPipelineRunner runner = returnExitCode(1);

        PublishJarsToRepo publishJarsToRepo = genericBuilder
                .commandRunner(runner)
                .clientJars(List.of(clientJar1))
                .lambdaJars(List.of(lambdaJar1))
                .build();

        //When/Then
        assertThatThrownBy(() -> publishJarsToRepo.upload())
                .isInstanceOf(CommandFailedException.class);

        assertThat(commandsThatRan).isEmpty();
    }

    private PublishJarsToRepo.Builder generateGenericBuilder() {
        return PublishJarsToRepo.builder()
                .jarsDirectory(Path.of("/some/directory/"))
                .repoUrl("someUrl")
                .version("0.31.0")
                .m2SettingsServerId("repo.id")
                .commandRunner(commandRunner);
    }

    private CommandPipeline generateMavenDeployFileCommand(String filename, String artifactId) {
        return pipeline(command(generateMavenCommands(filename, artifactId)));
    }

    private String[] generateMavenCommands(String filename, String artifactId) {
        return new String[]{"mvn", "deploy:deploy-file", "-q",
            "-Durl=someUrl",
            "-DrepositoryId=repo.id",
            "-Dfile=/some/directory/" + filename,
            "-DgroupId=sleeper",
            "-DartifactId=" + artifactId,
            "-Dversion=0.31.0",
            "-DgeneratePom=false"};
    }
}
