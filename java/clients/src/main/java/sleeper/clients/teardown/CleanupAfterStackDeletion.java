/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.clients.teardown;

import com.amazonaws.services.ecr.AmazonECR;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.util.ClientUtils;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static sleeper.configuration.properties.instance.CommonProperty.JARS_BUCKET;

public class CleanupAfterStackDeletion {
    public static final Logger LOGGER = LoggerFactory.getLogger(CleanupAfterStackDeletion.class);

    private final S3Client s3Client;
    private final AmazonECR ecrClient;
    private final Path generatedDir;

    public CleanupAfterStackDeletion(S3Client s3Client, AmazonECR ecrClient, Path generatedDir) {
        this.s3Client = s3Client;
        this.ecrClient = ecrClient;
        this.generatedDir = generatedDir;
    }

    public CleanupAfterStackDeletion(TearDownClients clients, Path scriptsDir) {
        this(clients.getS3v2(), clients.getEcr(), scriptsDir.resolve("generated"));
    }

    public void cleanup(InstanceProperties instanceProperties, List<String> extraEcrRepositories) throws InterruptedException, IOException {
        LOGGER.info("Removing the Jars bucket and docker containers");
        RemoveJarsBucket.remove(s3Client, instanceProperties.get(JARS_BUCKET));
        RemoveECRRepositories.remove(ecrClient, instanceProperties, extraEcrRepositories);

        if (Files.isDirectory(generatedDir)) {
            LOGGER.info("Removing generated files");
            ClientUtils.clearDirectory(generatedDir);
        } else {
            LOGGER.info("Generated directory not found");
        }
    }

}
