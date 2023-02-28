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

package sleeper.clients.admin.deploy;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.local.LoadLocalProperties;
import sleeper.configuration.properties.local.SaveLocalProperties;
import sleeper.core.SleeperVersion;

import java.io.IOException;
import java.nio.file.Path;

public class UpdateExistingInstance {
    private final Path scriptsDirectory;
    private final String instanceId;
    private final AmazonS3 s3;
    private final S3Client s3v2;

    private UpdateExistingInstance(Builder builder) {
        scriptsDirectory = builder.scriptsDirectory;
        instanceId = builder.instanceId;
        s3 = builder.s3;
        s3v2 = builder.s3v2;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (2 != args.length) {
            throw new IllegalArgumentException("Usage: <scripts-dir> <instance-id>");
        }
        Path scriptsDirectory = Path.of(args[0]);

        AmazonS3 s3 = AmazonS3Client.builder().build();
        try (S3Client s3v2 = S3Client.create()) {
            builder().s3(s3)
                    .s3v2(s3v2)
                    .instanceId(args[1])
                    .scriptsDirectory(scriptsDirectory)
                    .build().update();
        }
    }

    public void update() throws IOException, InterruptedException {
        // Get instance properties from s3
        Path generatedDirectory = scriptsDirectory.resolve("generated");
        Path jarsDirectory = scriptsDirectory.resolve("jars");
        SaveLocalProperties.saveFromS3(s3, instanceId, generatedDirectory);
        InstanceProperties properties = LoadLocalProperties.loadInstanceProperties(new InstanceProperties(), generatedDirectory);

        // Upload JARs to bucket
        SyncJars.builder().s3(s3v2)
                .jarsDirectory(jarsDirectory).instanceProperties(properties)
                .deleteOldJars(false)
                .build().sync();

        // Run CDK deploy
        CdkDeployInstance.builder()
                .instancePropertiesFile(generatedDirectory.resolve("instance.properties"))
                .version(SleeperVersion.getVersion())
                .jarsDirectory(jarsDirectory)
                .ensureNewInstance(false).skipVersionCheck(true)
                .build().deployInferringType(properties);
    }

    public static final class Builder {
        private Path scriptsDirectory;
        private String instanceId;
        private AmazonS3 s3;
        private S3Client s3v2;

        private Builder() {
        }

        public Builder scriptsDirectory(Path scriptsDirectory) {
            this.scriptsDirectory = scriptsDirectory;
            return this;
        }

        public Builder instanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Builder s3(AmazonS3 s3) {
            this.s3 = s3;
            return this;
        }

        public Builder s3v2(S3Client s3v2) {
            this.s3v2 = s3v2;
            return this;
        }

        public UpdateExistingInstance build() {
            return new UpdateExistingInstance(this);
        }
    }
}
