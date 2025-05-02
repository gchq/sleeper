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
package sleeper.clients.admin;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.emr.EmrClient;

import sleeper.clients.admin.properties.AdminClientPropertiesStore;
import sleeper.clients.admin.properties.UpdatePropertiesWithTextEditor;
import sleeper.clients.deploy.container.EcrRepositoryCreator;
import sleeper.clients.deploy.container.UploadDockerImages;
import sleeper.clients.report.ingest.job.PersistentEMRStepCount;
import sleeper.clients.util.AwsV2ClientHelper;
import sleeper.clients.util.cdk.InvokeCdkForInstance;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.clients.util.console.ConsoleOutput;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.configuration.utils.AwsV1ClientHelper;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.table.TableIndex;
import sleeper.task.common.QueueMessageCount;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Function;

public class AdminClient {
    private final TableIndex tableIndex;
    private final AdminClientPropertiesStore store;
    private final AdminClientTrackerFactory trackers;
    private final UpdatePropertiesWithTextEditor editor;
    private final ConsoleOutput out;
    private final ConsoleInput in;
    private final QueueMessageCount.Client queueClient;
    private final Function<InstanceProperties, Map<String, Integer>> getStepCount;

    public AdminClient(TableIndex tableIndex, AdminClientPropertiesStore store, AdminClientTrackerFactory trackers,
            UpdatePropertiesWithTextEditor editor, ConsoleOutput out, ConsoleInput in,
            QueueMessageCount.Client queueClient, Function<InstanceProperties, Map<String, Integer>> getStepCount) {
        this.tableIndex = tableIndex;
        this.store = store;
        this.trackers = trackers;
        this.editor = editor;
        this.out = out;
        this.in = in;
        this.queueClient = queueClient;
        this.getStepCount = getStepCount;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (2 != args.length) {
            throw new IllegalArgumentException("Usage: <scripts-dir> <instance-id>");
        }

        Path scriptsDir = Path.of(args[0]);
        String instanceId = args[1];
        Path generatedDir = scriptsDir.resolve("generated");
        Path jarsDir = scriptsDir.resolve("jars");
        Path baseDockerDir = scriptsDir.resolve("docker");
        String version = Files.readString(scriptsDir.resolve("templates/version.txt"));
        InvokeCdkForInstance cdk = InvokeCdkForInstance.builder()
                .propertiesFile(generatedDir.resolve("instance.properties"))
                .jarsDirectory(jarsDir).version(version).build();

        ConsoleOutput out = new ConsoleOutput(System.out);
        ConsoleInput in = new ConsoleInput(System.console());
        AmazonS3 s3Client = AwsV1ClientHelper.buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoClient = AwsV1ClientHelper.buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        AmazonSQS sqsClient = AwsV1ClientHelper.buildAwsV1Client(AmazonSQSClientBuilder.standard());

        int errorCode;
        try (EcrClient ecrClient = AwsV2ClientHelper.buildAwsV2Client(EcrClient.builder());
                EmrClient emrClient = AwsV2ClientHelper.buildAwsV2Client(EmrClient.builder())) {
            UploadDockerImages uploadDockerImages = UploadDockerImages.builder()
                    .ecrClient(EcrRepositoryCreator.withEcrClient(ecrClient))
                    .baseDockerDirectory(baseDockerDir).jarsDirectory(jarsDir).build();
            errorCode = start(instanceId, s3Client, dynamoClient, cdk, generatedDir, uploadDockerImages, out, in,
                    new UpdatePropertiesWithTextEditor(Path.of("/tmp")),
                    QueueMessageCount.withSqsClient(sqsClient),
                    properties -> PersistentEMRStepCount.byStatus(properties, emrClient));
        } finally {
            s3Client.shutdown();
            dynamoClient.shutdown();
            sqsClient.shutdown();
        }
        System.exit(errorCode);
    }

    public static int start(String instanceId, AmazonS3 s3Client, AmazonDynamoDB dynamoDB,
            InvokeCdkForInstance cdk, Path generatedDir, UploadDockerImages uploadDockerImages,
            ConsoleOutput out, ConsoleInput in, UpdatePropertiesWithTextEditor editor,
            QueueMessageCount.Client queueClient,
            Function<InstanceProperties, Map<String, Integer>> getStepCount) throws InterruptedException {
        AdminClientPropertiesStore store = new AdminClientPropertiesStore(
                s3Client, dynamoDB, cdk, generatedDir, uploadDockerImages);
        InstanceProperties instanceProperties;
        try {
            instanceProperties = store.loadInstanceProperties(instanceId);
        } catch (AdminClientPropertiesStore.CouldNotLoadInstanceProperties e) {
            e.print(out);
            return 1;
        }
        new AdminClient(
                new DynamoDBTableIndex(instanceProperties, dynamoDB),
                store,
                AdminClientTrackerFactory.from(dynamoDB),
                editor,
                out, in,
                queueClient, getStepCount)
                .start(instanceId);
        return 0;
    }

    public void start(String instanceId) throws InterruptedException {
        new AdminMainScreen(out, in).mainLoop(this, instanceId);
    }

    public InstanceConfigurationScreen instanceConfigurationScreen() {
        return new InstanceConfigurationScreen(out, in, store, editor);
    }

    public TableNamesReport tableNamesReport() {
        return new TableNamesReport(out, in, tableIndex);
    }

    public PartitionsStatusReportScreen partitionsStatusReportScreen() {
        return new PartitionsStatusReportScreen(out, in, store);
    }

    public FilesStatusReportScreen filesStatusReportScreen() {
        return new FilesStatusReportScreen(out, in, store);
    }

    public CompactionStatusReportScreen compactionStatusReportScreen() {
        return new CompactionStatusReportScreen(out, in, store, trackers);
    }

    public IngestStatusReportScreen ingestStatusReportScreen() {
        return new IngestStatusReportScreen(out, in, store, trackers, queueClient, getStepCount);
    }

    public IngestBatcherReportScreen ingestBatcherReportScreen() {
        return new IngestBatcherReportScreen(out, in, tableIndex, store, trackers);
    }
}
