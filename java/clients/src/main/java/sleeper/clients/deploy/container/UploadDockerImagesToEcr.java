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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.clients.util.command.Command.command;
import static sleeper.clients.util.command.CommandPipeline.pipeline;

/**
 * Uploads Docker images to individual AWS ECR repositories. Authenticates with AWS ECR, creates ECR repositories, and
 * compares the images to be uploaded with those already present.
 */
public class UploadDockerImagesToEcr {
    public static final Logger LOGGER = LoggerFactory.getLogger(UploadDockerImagesToEcr.class);

    private final UploadDockerImages uploader;
    private final CheckVersionExistsInEcr repositoryChecker;

    public UploadDockerImagesToEcr(UploadDockerImages uploader, CheckVersionExistsInEcr repositoryChecker) {
        this.uploader = uploader;
        this.repositoryChecker = repositoryChecker;
    }

    public void upload(UploadDockerImagesToEcrRequest request) throws IOException, InterruptedException {
        List<StackDockerImage> requestedImages = request.getImages();
        LOGGER.info("Images expected: {}", requestedImages);
        List<StackDockerImage> imagesToUpload = requestedImages.stream()
                .filter(image -> imageDoesNotExistInRepositoryWithVersion(image, request))
                .collect(Collectors.toUnmodifiableList());
        String repositoryPrefix = request.getRepositoryHost() + "/" + request.getEcrPrefix();
        if (!imagesToUpload.isEmpty()) {
            uploader.getCommandRunner().runOrThrow(pipeline(
                    command("aws", "ecr", "get-login-password", "--region", request.getRegion()),
                    command("docker", "login", "--username", "AWS", "--password-stdin", request.getRepositoryHost())));
        }
        uploader.upload(repositoryPrefix, imagesToUpload);
    }

    private boolean imageDoesNotExistInRepositoryWithVersion(
            StackDockerImage stackDockerImage, UploadDockerImagesToEcrRequest request) {
        String imagePath = request.getEcrPrefix() + "/" + stackDockerImage.getImageName();
        if (repositoryChecker.versionExistsInRepository(imagePath, uploader.getVersion())) {
            LOGGER.info("Stack image {} already exists in ECR with version {}",
                    stackDockerImage.getImageName(), uploader.getVersion());
            return false;
        } else {
            LOGGER.info("Stack image {} does not exist in ECR with version {}",
                    stackDockerImage.getImageName(), uploader.getVersion());
            return true;
        }
    }

}
