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
package sleeper.clients.deploy;

import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.deploy.container.CheckVersionExistsInEcr;
import sleeper.clients.deploy.container.UploadDockerImages;
import sleeper.clients.deploy.container.UploadDockerImagesToEcr;
import sleeper.clients.deploy.container.UploadDockerImagesToEcrRequest;
import sleeper.clients.deploy.jar.SyncJars;
import sleeper.clients.deploy.jar.SyncJarsRequest;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.local.LoadLocalProperties;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Uploads jars and Docker images to AWS. The S3 jars bucket and the ECR repositories must already have been created,
 * e.g. by deploying SleeperArtefactsCdkApp.
 */
public class UploadArtefacts {

    private UploadArtefacts() {
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (1 != args.length) {
            throw new IllegalArgumentException("Usage: <scripts directory> <instance.properties file>");
        }

        Path scriptsDir = Path.of(args[0]);
        Path propertiesFile = Path.of(args[1]);
        InstanceProperties instanceProperties = LoadLocalProperties.loadInstanceProperties(propertiesFile);

        try (S3Client s3Client = S3Client.create();
                EcrClient ecrClient = EcrClient.create()) {
            SyncJars syncJars = SyncJars.fromScriptsDirectory(s3Client, scriptsDir);
            UploadDockerImagesToEcr uploadImages = new UploadDockerImagesToEcr(
                    UploadDockerImages.fromScriptsDirectory(scriptsDir),
                    CheckVersionExistsInEcr.withEcrClient(ecrClient));
            syncJars.sync(SyncJarsRequest.from(instanceProperties));
            uploadImages.upload(UploadDockerImagesToEcrRequest.forDeployment(instanceProperties));
        }
    }

}
