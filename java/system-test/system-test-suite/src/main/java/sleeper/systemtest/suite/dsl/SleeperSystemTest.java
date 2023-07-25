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
import sleeper.configuration.properties.table.TableProperties;
import sleeper.systemtest.drivers.ingest.IngestSourceFilesContext;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.instance.SystemTestParameters;
import sleeper.systemtest.drivers.query.DirectQueryDriver;
import sleeper.systemtest.suite.fixtures.SystemTestInstance;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Consumer;

import static sleeper.systemtest.drivers.util.InvokeSystemTestLambda.createSystemTestLambdaClient;

/**
 * This class is the entry point that all system tests use to interact with the system.
 * It's the starting point for all steps of the Domain Specific Language (DSL) for system tests.
 * The purpose of this is to make it as easy as possible to read and write system tests.
 * It should make it easy to find and reuse steps to interact with the systems you care about for your tests.
 * <p>
 * It does this by delegating to drivers to actually interact with the system. The DSL only defines
 * the steps and language to be used by tests, and not any of the implemented behaviour of the steps.
 * <p>
 * This class should match as closely as possible with a diagram of the components of the system. You can
 * expect to find the system you care about through a method on this class. Some core features of the system are
 * on this class directly, but we try to limit its size by grouping methods into components which you access through
 * a method, eg. {@link #ingest()}.
 * <p>
 * Most tests will use steps from different systems, but where multiple steps run against the same system in succession
 * we can use method chaining to avoid accessing that system repeatedly. If you go from one system to another and back,
 * assume you should re-access the first system from here again the second time.
 * Try to avoid assigning variables except for data you want to reuse.
 */
public class SleeperSystemTest {

    private static final SleeperSystemTest INSTANCE = new SleeperSystemTest();

    private final SystemTestParameters parameters = SystemTestParameters.loadFromSystemProperties();
    private final CloudFormationClient cloudFormationClient = CloudFormationClient.create();
    private final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
    private final S3Client s3ClientV2 = S3Client.create();
    private final AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
    private final AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
    private final LambdaClient lambdaClient = createSystemTestLambdaClient();
    private final SleeperInstanceContext instance = new SleeperInstanceContext(
            parameters, cloudFormationClient, s3Client, dynamoDBClient);
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

    public void updateTableProperties(Consumer<TableProperties> tablePropertiesConsumer) {
        tablePropertiesConsumer.accept(instance.getTableProperties());
        try {
            instance.getTableProperties().saveToS3(s3Client);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public SystemTestSourceFiles sourceFiles() {
        return new SystemTestSourceFiles(instance, sourceFiles);
    }

    public SystemTestIngest ingest() {
        return new SystemTestIngest(parameters, instance, dynamoDBClient, sqsClient, lambdaClient);
    }

    public SystemTestDirectQuery directQuery() {
        return new SystemTestDirectQuery(new DirectQueryDriver(instance));
    }
}
