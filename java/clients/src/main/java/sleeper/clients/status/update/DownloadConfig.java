/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.clients.status.update;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.apache.commons.io.file.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.deploy.GenerateInstanceProperties;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.local.SaveLocalProperties;
import sleeper.configuration.properties.table.TableProperties;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class DownloadConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(DownloadConfig.class);

    private DownloadConfig() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            throw new IllegalArgumentException("Usage: <instance id> <directory to write to>");
        }
        String instanceId = args[0];
        Path basePath = Path.of(args[1]);
        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        LOGGER.info("Downloading configuration from S3");
        try {
            SaveLocalProperties.saveFromS3(s3, instanceId, basePath);
            LOGGER.info("Download complete");
        } catch (IOException e) {
            LOGGER.error("Download failed: {}", e.getMessage());
        }
    }

    public static InstanceProperties overwriteTargetDirectoryIfDownloadSuccessful(AmazonS3 s3, String instanceId, Path targetDir, Path tempDir) throws IOException {
        return overwriteTargetDirectoryIfSuccessful(targetDir, tempDir, save -> {
            InstanceProperties properties = new InstanceProperties();
            properties.loadFromS3GivenInstanceId(s3, instanceId);
            save.save(properties, TableProperties.streamTablesFromS3(s3, properties));
            return properties;
        });
    }

    public static InstanceProperties overwriteTargetDirectoryGenerateDefaultsIfMissing(
            AmazonS3 s3, String instanceId, Path targetDir, Path tempDir) throws IOException {
        return overwriteTargetDirectoryIfSuccessful(targetDir, tempDir, save -> {
            try {
                InstanceProperties properties = new InstanceProperties();
                properties.loadFromS3GivenInstanceId(s3, instanceId);
                save.save(properties, TableProperties.streamTablesFromS3(s3, properties));
                return properties;
            } catch (AmazonS3Exception e) {
                LOGGER.info("Failed to download configuration, using default properties");
                InstanceProperties properties = GenerateInstanceProperties.generateDefaultsFromInstanceId(instanceId);
                save.save(properties, Stream.empty());
                return properties;
            }
        });
    }

    public static InstanceProperties overwriteTargetDirectoryIfSuccessful(
            Path targetDir, Path tempDir, LoadInstanceProperties loadProperties) throws IOException {
        Files.createDirectories(tempDir);
        PathUtils.cleanDirectory(tempDir);
        InstanceProperties instanceProperties = loadProperties.load((properties, tables) ->
                SaveLocalProperties.saveToDirectory(tempDir, properties, tables));
        Files.createDirectories(targetDir);
        PathUtils.cleanDirectory(targetDir);
        PathUtils.copyDirectory(tempDir, targetDir);
        PathUtils.cleanDirectory(tempDir);
        return instanceProperties;
    }

    interface LoadInstanceProperties {
        InstanceProperties load(SaveInstanceProperties save) throws IOException;
    }

    interface SaveInstanceProperties {
        void save(InstanceProperties properties, Stream<TableProperties> tables) throws IOException;
    }
}
