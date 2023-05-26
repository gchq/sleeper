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

package sleeper.systemtest.ingest.batcher;

import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.deploy.DeployNewInstance;
import sleeper.clients.util.cdk.InvokeCdkForInstance;

import java.io.IOException;
import java.nio.file.Path;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_SOURCE_BUCKET;

public class SystemTestForIngestBatcher {
    private final Path scriptsDir;
    private final Path propertiesTemplate;
    private final String instanceId;
    private final String vpc;
    private final String subnet;
    private final S3Client s3Client;

    private SystemTestForIngestBatcher(Builder builder) {
        scriptsDir = builder.scriptsDir;
        propertiesTemplate = builder.propertiesTemplate;
        instanceId = builder.instanceId;
        vpc = builder.vpc;
        subnet = builder.subnet;
        s3Client = builder.s3Client;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Create an S3 bucket
     * Add it as an ingest source bucket in a Sleeper instance
     * Deploy instance
     * Submit files to the ingest batcher
     * Trigger the ingest batcher to create jobs
     * Test ingest via both standard ingest and bulk import
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 5) {
            throw new IllegalArgumentException("Usage: <scripts-dir> <properties-template> <instance-id> <vpc> <subnet>");
        }
        try (S3Client s3Client = S3Client.create()) {
            builder().scriptsDir(Path.of(args[0]))
                    .propertiesTemplate(Path.of(args[1]))
                    .instanceId(args[2])
                    .vpc(args[3])
                    .subnet(args[4])
                    .s3Client(s3Client)
                    .build().run();
        }
    }

    public void run() throws IOException, InterruptedException {
        String sourceBucketName = "sleeper-" + instanceId + "-ingest-source";
        createS3Bucket(sourceBucketName);

        DeployNewInstance.builder().scriptsDirectory(scriptsDir)
                .instancePropertiesTemplate(propertiesTemplate)
                .extraInstanceProperties(properties ->
                        properties.setProperty(INGEST_SOURCE_BUCKET.getPropertyName(), sourceBucketName))
                .instanceId(instanceId)
                .vpcId(vpc)
                .subnetId(subnet)
                .deployPaused(true)
                .tableName("system-test")
                .instanceType(InvokeCdkForInstance.Type.STANDARD)
                .deployWithDefaultClients();
    }

    private static void createS3Bucket(String sourceBucketName) {
        try (S3Client s3Client = S3Client.create()) {
            s3Client.createBucket(builder -> builder.bucket(sourceBucketName));
        }
    }

    public static final class Builder {
        private Path scriptsDir;
        private Path propertiesTemplate;
        private String instanceId;
        private String vpc;
        private String subnet;
        private S3Client s3Client;

        private Builder() {
        }


        public Builder scriptsDir(Path scriptsDir) {
            this.scriptsDir = scriptsDir;
            return this;
        }

        public Builder propertiesTemplate(Path propertiesTemplate) {
            this.propertiesTemplate = propertiesTemplate;
            return this;
        }

        public Builder instanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Builder vpc(String vpc) {
            this.vpc = vpc;
            return this;
        }

        public Builder subnet(String subnet) {
            this.subnet = subnet;
            return this;
        }

        public Builder s3Client(S3Client s3Client) {
            this.s3Client = s3Client;
            return this;
        }

        public SystemTestForIngestBatcher build() {
            return new SystemTestForIngestBatcher(this);
        }
    }
}
