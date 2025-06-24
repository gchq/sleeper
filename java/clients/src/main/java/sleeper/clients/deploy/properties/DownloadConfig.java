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
package sleeper.clients.deploy.properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.configuration.properties.S3InstanceProperties;

import java.io.IOException;
import java.nio.file.Path;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;

public class DownloadConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(DownloadConfig.class);

    private DownloadConfig() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            throw new IllegalArgumentException("Usage: <instance-id> <directory-to-write-to>");
        }
        String instanceId = args[0];
        Path basePath = Path.of(args[1]);
        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder())) {
            LOGGER.info("Downloading configuration from S3");
            S3InstanceProperties.saveToLocalWithTableProperties(s3Client, dynamoClient, instanceId, basePath);
            LOGGER.info("Download complete");
        }
    }

}
