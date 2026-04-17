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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.util.command.CommandUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static sleeper.clients.util.command.Command.command;
import static sleeper.clients.util.command.CommandPipeline.pipeline;

/**
 * Uploads Docker images to individual AWS ECR repositories. Authenticates with AWS ECR, creates ECR repositories, and
 * compares the images to be uploaded with those already present.
 */
public class UploadDockerImagesToEcr {
    public static final Logger LOGGER = LoggerFactory.getLogger(UploadDockerImagesToEcr.class);

    private final UploadDockerImages uploader;
    private final String account;
    private final String region;
    private final String repositoryHost;

    public UploadDockerImagesToEcr(UploadDockerImages uploader, String account, String region) {
        this.uploader = uploader;
        this.account = account;
        this.region = region;
        this.repositoryHost = String.format("%s.dkr.ecr.%s.amazonaws.com", this.account, this.region);
    }

    public void upload(UploadDockerImagesToEcrRequest request) throws IOException, InterruptedException {
        List<StackDockerImage> requestedImages = request.getImages();
        LOGGER.info("Images expected: {}", requestedImages);
        String repositoryPrefix = repositoryHost + "/" + request.getEcrPrefix();
        if (!requestedImages.isEmpty()) {
            uploader.getCommandRunner().runOrThrow(pipeline(
                    command("aws", "ecr", "get-login-password", "--region", region),
                    command("docker", "login", "--username", "AWS", "--password-stdin", repositoryHost)));
        }
        uploader.upload(repositoryPrefix, requestedImages, request.isOverwriteExistingTag());
    }

    private void populateDockerDigests(String instanceId, List<StackDockerImage> images) {
        //Run docker images --filter "reference=* /* /{{INSTANT_ID}}/*" --digests --format "{{.Repository}} {{.Tag}} {{.Digest}}"
        try {
            List<String> digests = CommandUtils.runCommandReturnOutput("docker", "images",
                    "--filter", "reference=*/" + instanceId + "/*", "--format", "{{.Repository}},{{.Digest}}");

            Map<String, String> imageNameToDigest = new HashMap<>();
            for (String s : digests) {
                String imageName = s.split(",")[0];
                imageName = imageName.split("/")[2].split(":")[0];
                String digest = s.split(",")[1];
                imageNameToDigest.put(imageName, digest);
            }
        } catch (IOException | InterruptedException | ExecutionException e) {
            // 6671 TODO - This exception handling could be moved into CommandUtils
            // and/or we could just not check ECR for the digest if not found. Doing so would probably help 5852
        }
    }
}
