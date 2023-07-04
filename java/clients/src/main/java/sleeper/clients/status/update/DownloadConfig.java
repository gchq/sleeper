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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.local.SaveLocalProperties;

import java.io.IOException;
import java.nio.file.Path;

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

}
