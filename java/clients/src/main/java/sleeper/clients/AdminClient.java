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
package sleeper.clients;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import sleeper.clients.admin.AdminClientPropertiesStore;
import sleeper.clients.admin.AdminClientStatusStoreFactory;
import sleeper.clients.admin.AdminMainScreen;
import sleeper.clients.admin.CompactionStatusReportScreen;
import sleeper.clients.admin.FilesStatusReportScreen;
import sleeper.clients.admin.IngestBatcherReportScreen;
import sleeper.clients.admin.IngestStatusReportScreen;
import sleeper.clients.admin.InstanceConfigurationScreen;
import sleeper.clients.admin.PartitionsStatusReportScreen;
import sleeper.clients.admin.TableNamesReport;
import sleeper.clients.admin.UpdatePropertiesWithNano;
import sleeper.clients.status.report.ingest.job.PersistentEMRStepCount;
import sleeper.clients.util.cdk.InvokeCdkForInstance;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.clients.util.console.ConsoleOutput;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.utils.AwsV1ClientHelper;
import sleeper.job.common.QueueMessageCount;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Function;

public class AdminClient {

    private final AdminClientPropertiesStore store;
    private final AdminClientStatusStoreFactory statusStores;
    private final UpdatePropertiesWithNano editor;
    private final ConsoleOutput out;
    private final ConsoleInput in;
    private final QueueMessageCount.Client queueClient;
    private final Function<InstanceProperties, Map<String, Integer>> getStepCount;

    public AdminClient(AdminClientPropertiesStore store, AdminClientStatusStoreFactory statusStores,
                       UpdatePropertiesWithNano editor, ConsoleOutput out, ConsoleInput in,
                       QueueMessageCount.Client queueClient, Function<InstanceProperties, Map<String, Integer>> getStepCount) {
        this.store = store;
        this.statusStores = statusStores;
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
        InvokeCdkForInstance cdk = InvokeCdkForInstance.builder()
                .propertiesFile(generatedDir.resolve("instance.properties"))
                .jarsDirectory(jarsDir).version(version).build();

        AmazonS3 s3Client = AwsV1ClientHelper.buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDB = AwsV1ClientHelper.buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        AmazonSQS sqsClient = AwsV1ClientHelper.buildAwsV1Client(AmazonSQSClientBuilder.standard());
        new AdminClient(
                new AdminClientPropertiesStore(
                        s3Client,
                        dynamoDB,
                        cdk, generatedDir),
                AdminClientStatusStoreFactory.from(dynamoDB),
                new UpdatePropertiesWithNano(Path.of("/tmp")),
                new ConsoleOutput(System.out),
                new ConsoleInput(System.console()),
                QueueMessageCount.withSqsClient(sqsClient),
                (properties -> PersistentEMRStepCount.byStatus(properties, AmazonElasticMapReduceClientBuilder.defaultClient())))
                .start(instanceId);
    }

    public void start(String instanceId) throws InterruptedException {
        try {
            store.loadInstanceProperties(instanceId);
            new AdminMainScreen(out, in).mainLoop(this, instanceId);
        } catch (AdminClientPropertiesStore.CouldNotLoadInstanceProperties e) {
            e.print(out);
        }
    }

    public InstanceConfigurationScreen instanceConfigurationScreen() {
        return new InstanceConfigurationScreen(out, in, store, editor);
    }

    public TableNamesReport tableNamesReport() {
        return new TableNamesReport(out, in, store);
    }

    public PartitionsStatusReportScreen partitionsStatusReportScreen() {
        return new PartitionsStatusReportScreen(out, in, store);
    }

    public FilesStatusReportScreen filesStatusReportScreen() {
        return new FilesStatusReportScreen(out, in, store);
    }

    public CompactionStatusReportScreen compactionStatusReportScreen() {
        return new CompactionStatusReportScreen(out, in, store, statusStores);
    }

    public IngestStatusReportScreen ingestStatusReportScreen() {
        return new IngestStatusReportScreen(out, in, store, statusStores, queueClient, getStepCount);
    }

    public IngestBatcherReportScreen ingestBatcherReportScreen() {
        return new IngestBatcherReportScreen(out, in, store, statusStores);
    }
}
