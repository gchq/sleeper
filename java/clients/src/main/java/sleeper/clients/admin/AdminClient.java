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

import software.amazon.awssdk.regions.providers.AwsRegionProvider;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sts.StsClient;

import sleeper.clients.admin.properties.AdminClientPropertiesStore;
import sleeper.clients.admin.properties.UpdatePropertiesWithTextEditor;
import sleeper.clients.admin.screen.AdminMainScreen;
import sleeper.clients.admin.screen.CompactionStatusReportScreen;
import sleeper.clients.admin.screen.FilesStatusReportScreen;
import sleeper.clients.admin.screen.IngestBatcherReportScreen;
import sleeper.clients.admin.screen.IngestStatusReportScreen;
import sleeper.clients.admin.screen.InstanceConfigurationScreen;
import sleeper.clients.admin.screen.PartitionsStatusReportScreen;
import sleeper.clients.deploy.container.CheckVersionExistsInEcr;
import sleeper.clients.deploy.container.DockerImageConfiguration;
import sleeper.clients.deploy.container.UploadDockerImages;
import sleeper.clients.deploy.container.UploadDockerImagesToEcr;
import sleeper.clients.report.TableNamesReport;
import sleeper.clients.report.ingest.job.PersistentEMRStepCount;
import sleeper.clients.util.cdk.InvokeCdk;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.clients.util.console.ConsoleOutput;
import sleeper.common.task.QueueMessageCount;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.configuration.utils.AwsV2ClientHelper;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.table.TableIndex;

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
        String version = Files.readString(scriptsDir.resolve("templates/version.txt"));
        InvokeCdk cdk = InvokeCdk.builder()
                .jarsDirectory(jarsDir).version(version).build();

        ConsoleOutput out = new ConsoleOutput(System.out);
        ConsoleInput in = new ConsoleInput(System.console());

        int errorCode;
        try (S3Client s3Client = AwsV2ClientHelper.buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = AwsV2ClientHelper.buildAwsV2Client(DynamoDbClient.builder());
                SqsClient sqsClient = AwsV2ClientHelper.buildAwsV2Client(SqsClient.builder());
                EcrClient ecrClient = AwsV2ClientHelper.buildAwsV2Client(EcrClient.builder());
                EmrClient emrClient = AwsV2ClientHelper.buildAwsV2Client(EmrClient.builder());
                StsClient stsClient = AwsV2ClientHelper.buildAwsV2Client(StsClient.builder())) {
            AwsRegionProvider regionProvider = DefaultAwsRegionProviderChain.builder().build();
            UploadDockerImagesToEcr uploadDockerImages = new UploadDockerImagesToEcr(
                    UploadDockerImages.fromScriptsDirectory(scriptsDir),
                    CheckVersionExistsInEcr.withEcrClient(ecrClient),
                    stsClient.getCallerIdentity().account(), regionProvider.getRegion().id());
            errorCode = start(instanceId, s3Client, dynamoClient, cdk, generatedDir,
                    uploadDockerImages, DockerImageConfiguration.getDefault(), out, in,
                    new UpdatePropertiesWithTextEditor(Path.of("/tmp")),
                    QueueMessageCount.withSqsClient(sqsClient),
                    properties -> PersistentEMRStepCount.byStatus(properties, emrClient));
        }
        System.exit(errorCode);
    }

    public static int start(String instanceId,
            S3Client s3Client, DynamoDbClient dynamoClient, InvokeCdk cdk, Path generatedDir,
            UploadDockerImagesToEcr uploadDockerImages, DockerImageConfiguration dockerImageConfiguration,
            ConsoleOutput out, ConsoleInput in, UpdatePropertiesWithTextEditor editor,
            QueueMessageCount.Client queueClient,
            Function<InstanceProperties, Map<String, Integer>> getStepCount) throws InterruptedException {
        AdminClientPropertiesStore store = new AdminClientPropertiesStore(
                s3Client, dynamoClient, cdk, generatedDir, uploadDockerImages, dockerImageConfiguration);
        InstanceProperties instanceProperties;
        try {
            instanceProperties = store.loadInstanceProperties(instanceId);
        } catch (AdminClientPropertiesStore.CouldNotLoadInstanceProperties e) {
            e.print(out);
            return 1;
        }
        new AdminClient(
                new DynamoDBTableIndex(instanceProperties, dynamoClient),
                store,
                AdminClientTrackerFactory.from(dynamoClient),
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
        return new InstanceConfigurationScreen(out, in, store, editor, tableNamesReport());
    }

    public TableNamesReport tableNamesReport() {
        return new TableNamesReport(out, in, tableIndex);
    }

    public PartitionsStatusReportScreen partitionsStatusReportScreen() {
        return new PartitionsStatusReportScreen(out, in, store, tableNamesReport());
    }

    public FilesStatusReportScreen filesStatusReportScreen() {
        return new FilesStatusReportScreen(out, in, store, tableNamesReport());
    }

    public CompactionStatusReportScreen compactionStatusReportScreen() {
        return new CompactionStatusReportScreen(out, in, store, trackers, tableNamesReport());
    }

    public IngestStatusReportScreen ingestStatusReportScreen() {
        return new IngestStatusReportScreen(out, in, store, trackers, queueClient, getStepCount, tableNamesReport());
    }

    public IngestBatcherReportScreen ingestBatcherReportScreen() {
        return new IngestBatcherReportScreen(out, in, tableIndex, store, trackers);
    }
}
