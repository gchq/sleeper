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

package sleeper.systemtest.suite.dsl;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.systemtest.drivers.ingest.DirectIngestDriver;
import sleeper.systemtest.drivers.ingest.IngestBatcherDriver;
import sleeper.systemtest.drivers.ingest.IngestSourceFilesContext;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.instance.SystemTestParameters;
import sleeper.systemtest.drivers.query.DirectQueryDriver;
import sleeper.systemtest.suite.fixtures.SystemTestInstance;

import java.nio.file.Path;

import static sleeper.systemtest.drivers.util.InvokeSystemTestLambda.createSystemTestLambdaClient;

public class SleeperSystemTest {

    private static final SleeperSystemTest INSTANCE = new SleeperSystemTest();

    private final SystemTestParameters parameters = SystemTestParameters.loadFromSystemProperties();
    private final CloudFormationClient cloudFormationClient = CloudFormationClient.create();
    private final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
    private final S3Client s3ClientV2 = S3Client.create();
    private final AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.defaultClient();
    private final AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
    private final LambdaClient lambda = createSystemTestLambdaClient();
    private final SleeperInstanceContext instance = new SleeperInstanceContext(
            parameters, cloudFormationClient, s3Client, dynamoDB);
    private final IngestSourceFilesContext sourceFiles = new IngestSourceFilesContext(parameters, s3ClientV2);

    public SleeperSystemTest() {
        sourceFiles.createOrEmptySourceBucket();
    }

    public static SleeperSystemTest getInstance() {
        return INSTANCE;
    }

    public void connectToInstance(SystemTestInstance testInstance) {
        instance.connectTo(testInstance.getIdentifier(), testInstance.getInstanceConfiguration(parameters));
        instance.reinitialise();
    }

    public InstanceProperties instanceProperties() {
        return instance.getInstanceProperties();
    }

    public SystemTestDirectIngest directIngest(Path tempDir) {
        return new SystemTestDirectIngest(new DirectIngestDriver(instance, tempDir));
    }

    public SystemTestDirectQuery directQuery() {
        return new SystemTestDirectQuery(new DirectQueryDriver(instance));
    }

    public SystemTestSourceFiles sourceFiles() {
        return new SystemTestSourceFiles(instance, sourceFiles);
    }

    public SystemTestIngestBatcher ingestBatcher() {
        return new SystemTestIngestBatcher(parameters, instance, new IngestBatcherDriver(instance, dynamoDB, sqsClient, lambda));
    }
}
