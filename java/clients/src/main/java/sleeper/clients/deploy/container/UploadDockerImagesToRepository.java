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

import sleeper.clients.deploy.DeployConfiguration;

import java.io.IOException;
import java.nio.file.Path;

import static sleeper.clients.util.ClientUtils.optionalArgument;

public class UploadDockerImagesToRepository {

    private UploadDockerImagesToRepository() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 3) {
            System.out.println("Usage: <scripts-dir> <repository-prefix-path> <optional-create-buildx-builder-true-or-false>");
            System.exit(1);
            return;
        }

        Path scriptsDirectory = Path.of(args[0]);
        String repositoryPrefix = args[1];
        boolean createMultiplatformBuilder = optionalArgument(args, 2).map(Boolean::parseBoolean).orElse(true);

        UploadDockerImages uploader = UploadDockerImages.builder()
                .scriptsDirectory(scriptsDirectory)
                .deployConfig(DeployConfiguration.fromLocalBuild())
                .createMultiplatformBuilder(createMultiplatformBuilder)
                .build();
        uploadAllImages(DockerImageConfiguration.getDefault(), uploader, repositoryPrefix);
    }

    public static void uploadAllImages(DockerImageConfiguration imageConfig, UploadDockerImages uploader, String repositoryPrefix) throws IOException, InterruptedException {
        uploader.upload(repositoryPrefix, imageConfig.getAllImagesToUpload(), UploadDockerImagesCallbacks.NONE);
    }

}
