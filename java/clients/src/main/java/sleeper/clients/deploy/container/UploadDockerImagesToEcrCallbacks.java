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
package sleeper.clients.deploy.container;

import sleeper.clients.deploy.container.EcrRepositoryCreator.Client;
import sleeper.clients.util.command.CommandPipelineRunner;

import java.io.IOException;

import static sleeper.clients.util.command.Command.command;
import static sleeper.clients.util.command.CommandPipeline.pipeline;

public class UploadDockerImagesToEcrCallbacks implements UploadDockerImagesCallbacks {
    private final CommandPipelineRunner runCommand;
    private final EcrRepositoryCreator.Client ecrClient;
    private final UploadDockerImagesToEcrRequest request;

    public UploadDockerImagesToEcrCallbacks(CommandPipelineRunner runCommand, Client ecrClient, UploadDockerImagesToEcrRequest request) {
        this.runCommand = runCommand;
        this.ecrClient = ecrClient;
        this.request = request;
    }

    @Override
    public void beforeAll() throws IOException, InterruptedException {
        runCommand.runOrThrow(pipeline(
                command("aws", "ecr", "get-login-password", "--region", request.getRegion()),
                command("docker", "login", "--username", "AWS", "--password-stdin", request.getRepositoryHost())));
    }

    @Override
    public void beforeEach(StackDockerImage image) {
        String repositoryName = repositoryName(image);
        if (!ecrClient.repositoryExists(repositoryName)) {
            ecrClient.createRepository(repositoryName);
        }
        try {
            if (image.isCreateEmrServerlessPolicy()) {
                ecrClient.createEmrServerlessAccessPolicy(repositoryName);
            }
        } catch (Exception e) {
            ecrClient.deleteRepository(repositoryName);
            throw e;
        }
    }

    @Override
    public void onFail(StackDockerImage image, Exception e) {
        ecrClient.deleteRepository(repositoryName(image));
    }

    private String repositoryName(StackDockerImage image) {
        return request.getEcrPrefix() + "/" + image.getImageName();
    }

}
