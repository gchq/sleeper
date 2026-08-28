/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.clients.deploy.container;

import org.junit.jupiter.api.Test;

import sleeper.clients.deploy.UploadArtefacts;
import sleeper.clients.deploy.jar.SyncJarsRequest;
import sleeper.core.deploy.DockerDeployment;
import sleeper.core.util.cli.CommandArgumentReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class UploadArtefactsTest {

    private final List<String> artefactDeployments = new ArrayList<>();
    private final List<SyncJarsRequest> jarUploads = new ArrayList<>();
    private final List<UploadDockerImagesToEcrRequest> imageUploads = new ArrayList<>();
    private static final DockerDeployment DOCKER_DEPLOYMENT = DockerDeployment.builder()
            .deploymentName("test-image")
            .useDefaultBaseImage(false)
            .build();

    @Test
    void shouldUploadArtefactsToExistingDeploymentById() throws Exception {
        // When
        uploadArtefacts("--id", "test");

        // Then
        assertThat(artefactDeployments).isEmpty();
        assertThat(jarUploads).containsExactly(SyncJarsRequest.builder()
                .deploymentId("test")
                .build());
        assertThat(imageUploads).containsExactly(UploadDockerImagesToEcrRequest.builder()
                .ecrPrefix("test")
                .images(List.of(StackDockerImage.fromDockerDeployment(DOCKER_DEPLOYMENT)))
                .build());
    }

    @Test
    void shouldUploadArtefactsToNewDeploymentById() throws Exception {
        // When
        uploadArtefacts("--id", "test", "--create-deployment");

        // Then
        assertThat(artefactDeployments).containsExactly("test");
        assertThat(jarUploads).containsExactly(SyncJarsRequest.builder()
                .deploymentId("test")
                .build());
        assertThat(imageUploads).containsExactly(UploadDockerImagesToEcrRequest.builder()
                .ecrPrefix("test")
                .images(List.of(StackDockerImage.fromDockerDeployment(DOCKER_DEPLOYMENT)))
                .build());
    }

    @Test
    void shouldOnlyUploadJars() throws Exception {
        // When
        uploadArtefacts("--id", "test", "-u", "jars");

        // Then
        assertThat(artefactDeployments).isEmpty();
        assertThat(jarUploads).containsExactly(SyncJarsRequest.builder()
                .deploymentId("test")
                .build());
        assertThat(imageUploads).isEmpty();
    }

    @Test
    void shouldOnlyUploadImages() throws Exception {
        // When
        uploadArtefacts("--id", "test", "-u", "images");

        // Then
        assertThat(artefactDeployments).isEmpty();
        assertThat(jarUploads).isEmpty();
        assertThat(imageUploads).containsExactly(UploadDockerImagesToEcrRequest.builder()
                .ecrPrefix("test")
                .images(List.of(StackDockerImage.fromDockerDeployment(DOCKER_DEPLOYMENT)))
                .build());
    }

    @Test
    void shouldOverrideBaseImageRegistry() {
        // When
        var arguments = readArguments("--id", "test", "--base-image-registry", "my-registry");

        // Then
        assertThat(arguments.baseImageDestination())
                .isEqualTo(BaseImageDestination.fixedRegistry("my-registry"));
    }

    @Test
    void shouldDefaultBaseImageRegistry() {
        // When
        var arguments = readArguments("--id", "test");

        // Then
        assertThat(arguments.baseImageDestination())
                .isEqualTo(BaseImageDestination.managedRegistry(UploadDockerImages.BASE_IMAGE_REGISTRY_PORT));
    }

    @Test
    void shouldDisableMultiplatformBuilderCreation() {
        // When
        var arguments = readArguments("--id", "test", "--create-builder=false");

        // Then
        assertThat(arguments.createMultiplatformBuilder()).isFalse();
    }

    @Test
    void shouldDefaultMultiplatformBuilderCreation() {
        // When
        var arguments = readArguments("--id", "test");

        // Then
        assertThat(arguments.createMultiplatformBuilder()).isTrue();
    }

    private void uploadArtefacts(String... args) throws Exception {
        UploadArtefacts.upload(
                readArguments(args),
                new DockerImageConfiguration(List.of(DOCKER_DEPLOYMENT), List.of()),
                new FakeClient());
    }

    private UploadArtefacts.Arguments readArguments(String... args) {
        String[] allArgs = Stream.concat(Stream.of("./scripts"), Stream.of(args)).toArray(String[]::new);
        return UploadArtefacts.readArguments(CommandArgumentReader.parse(UploadArtefacts.USAGE, allArgs));
    }

    class FakeClient implements UploadArtefacts.Client {

        @Override
        public void deployArtefactRepositories(String deploymentId) {
            artefactDeployments.add(deploymentId);
        }

        @Override
        public void uploadJars(SyncJarsRequest request) throws IOException {
            jarUploads.add(request);
        }

        @Override
        public void uploadImages(UploadDockerImagesToEcrRequest request) throws IOException, InterruptedException {
            imageUploads.add(request);
        }

    }

}
