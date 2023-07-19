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

package sleeper.systemtest.drivers.ingest.batcher;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.cloudformation.model.CloudFormationException;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

import sleeper.clients.deploy.DeployNewInstance;
import sleeper.clients.util.cdk.InvokeCdkForInstance;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;
import sleeper.systemtest.drivers.util.InvokeSystemTestLambda;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.clients.deploy.DeployInstanceConfiguration.fromInstancePropertiesOrTemplatesDir;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_SOURCE_BUCKET;

public class SystemTestForIngestBatcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(SystemTestForIngestBatcher.class);
    private final Path scriptsDir;
    private final Path instancePropertiesPath;
    private final String instanceId;
    private final String vpc;
    private final String subnet;
    private final AmazonS3 s3ClientV1;
    private final S3Client s3ClientV2;
    private final CloudFormationClient cloudFormationClient;
    private final AmazonSQS sqsClient;
    private final LambdaClient lambdaClient;
    private final AmazonDynamoDB dynamoDB;

    private SystemTestForIngestBatcher(Builder builder) {
        scriptsDir = builder.scriptsDir;
        instancePropertiesPath = builder.instancePropertiesPath;
        instanceId = builder.instanceId;
        vpc = builder.vpc;
        subnet = builder.subnet;
        s3ClientV1 = builder.s3ClientV1;
        s3ClientV2 = builder.s3ClientV2;
        cloudFormationClient = builder.cloudFormationClient;
        sqsClient = builder.sqsClient;
        lambdaClient = builder.lambdaClient;
        dynamoDB = builder.dynamoDB;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static void main(String[] args) throws IOException, InterruptedException, StateStoreException {
        if (args.length != 5) {
            throw new IllegalArgumentException("Usage: <scripts-dir> <properties-file> <instance-id> <vpc> <subnet>");
        }
        try (S3Client s3Client = S3Client.create();
             CloudFormationClient cloudFormationClient = CloudFormationClient.create()) {
            builder().scriptsDir(Path.of(args[0]))
                    .instancePropertiesPath(Path.of(args[1]))
                    .instanceId(args[2])
                    .vpc(args[3])
                    .subnet(args[4])
                    .s3ClientV1(AmazonS3ClientBuilder.defaultClient())
                    .s3ClientV2(s3Client)
                    .cloudFormationClient(cloudFormationClient)
                    .sqsClient(AmazonSQSClientBuilder.defaultClient())
                    .lambdaClient(InvokeSystemTestLambda.createSystemTestLambdaClient())
                    .dynamoDB(AmazonDynamoDBClientBuilder.defaultClient())
                    .build().run();
        }
    }

    public void run() throws IOException, InterruptedException, StateStoreException {
        String sourceBucketName = "sleeper-" + instanceId + "-ingest-source";
        LOGGER.info("Creating bucket: {}", sourceBucketName);
        s3ClientV2.createBucket(builder -> builder.bucket(sourceBucketName));
        try {
            createInstanceIfMissing(sourceBucketName);

            InstanceProperties instanceProperties = new InstanceProperties();
            instanceProperties.loadFromS3GivenInstanceId(s3ClientV1, instanceId);
            TableProperties tableProperties = new TableProperties(instanceProperties);
            tableProperties.loadFromS3(s3ClientV1, "system-test");
            InvokeIngestBatcher invoke = new InvokeIngestBatcher(instanceProperties, tableProperties, sourceBucketName,
                    s3ClientV1, dynamoDB, sqsClient, lambdaClient);

            int activeFileCountBeforeStandardIngest = countActiveFiles(instanceProperties, tableProperties);
            invoke.runStandardIngest();
            int activeFileCountAfterStandardIngest = countActiveFiles(instanceProperties, tableProperties);
            checkActiveFilesChanged(activeFileCountBeforeStandardIngest, activeFileCountAfterStandardIngest, 2);

            invoke.runBulkImportEMR();
            checkActiveFilesChanged(activeFileCountAfterStandardIngest,
                    countActiveFiles(instanceProperties, tableProperties), 1);
        } finally {
            clearBucket(sourceBucketName);
            s3ClientV2.deleteBucket(builder -> builder.bucket(sourceBucketName));
        }
    }

    private void checkActiveFilesChanged(int activeFileCountBefore, int activeFileCountAfter, int expected) {
        int actualFiles = activeFileCountAfter - activeFileCountBefore;
        if (actualFiles != expected) {
            throw new IllegalStateException("Some files were not ingested correctly. " +
                    "Expected " + expected + " files but found " + actualFiles);
        }
    }

    // TODO Rename method?
    private int countActiveFiles(InstanceProperties properties, TableProperties tableProperties) throws StateStoreException {
        StateStoreProvider provider = new StateStoreProvider(dynamoDB, properties);
        return provider.getStateStore(tableProperties).getFileInPartitionList().size();
    }

    private void createInstanceIfMissing(String sourceBucketName) throws IOException, InterruptedException {
        try {
            cloudFormationClient.describeStacks(builder -> builder.stackName(instanceId));
            LOGGER.info("Instance already exists: {}", instanceId);
        } catch (CloudFormationException e) {
            LOGGER.info("Deploying instance: {}", instanceId);
            DeployNewInstance.builder().scriptsDirectory(scriptsDir)
                    .deployInstanceConfiguration(fromInstancePropertiesOrTemplatesDir(
                            instancePropertiesPath, scriptsDir.resolve("templates")))
                    .extraInstanceProperties(properties ->
                            properties.set(INGEST_SOURCE_BUCKET, sourceBucketName))
                    .instanceId(instanceId)
                    .vpcId(vpc)
                    .subnetIds(subnet)
                    .deployPaused(true)
                    .tableName("system-test")
                    .instanceType(InvokeCdkForInstance.Type.STANDARD)
                    .deployWithDefaultClients();
        }
    }

    private void clearBucket(String sourceBucketName) {
        List<ObjectIdentifier> objects = s3ClientV2.listObjectVersions(builder -> builder.bucket(sourceBucketName))
                .versions().stream()
                .map(obj -> ObjectIdentifier.builder().key(obj.key()).versionId(obj.versionId()).build())
                .collect(Collectors.toList());
        s3ClientV2.deleteObjects(builder -> builder.bucket(sourceBucketName)
                .delete(deleteBuilder -> deleteBuilder.objects(objects)));
    }

    public static final class Builder {
        private Path scriptsDir;
        private Path instancePropertiesPath;
        private String instanceId;
        private String vpc;
        private String subnet;
        private AmazonS3 s3ClientV1;
        private S3Client s3ClientV2;
        private CloudFormationClient cloudFormationClient;
        private AmazonSQS sqsClient;
        private LambdaClient lambdaClient;
        private AmazonDynamoDB dynamoDB;

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
        }

        public Builder scriptsDir(Path scriptsDir) {
            this.scriptsDir = scriptsDir;
            return this;
        }

        public Builder instancePropertiesPath(Path instancePropertiesPath) {
            this.instancePropertiesPath = instancePropertiesPath;
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

        public Builder s3ClientV1(AmazonS3 s3ClientV1) {
            this.s3ClientV1 = s3ClientV1;
            return this;
        }

        public Builder s3ClientV2(S3Client s3ClientV2) {
            this.s3ClientV2 = s3ClientV2;
            return this;
        }

        public Builder cloudFormationClient(CloudFormationClient cloudFormationClient) {
            this.cloudFormationClient = cloudFormationClient;
            return this;
        }

        public Builder sqsClient(AmazonSQS sqsClient) {
            this.sqsClient = sqsClient;
            return this;
        }

        public Builder lambdaClient(LambdaClient lambdaClient) {
            this.lambdaClient = lambdaClient;
            return this;
        }

        public Builder dynamoDB(AmazonDynamoDB dynamoDB) {
            this.dynamoDB = dynamoDB;
            return this;
        }

        public SystemTestForIngestBatcher build() {
            return new SystemTestForIngestBatcher(this);
        }
    }
}
