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
package sleeper.systemtest.datageneration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sts.StsClient;

import sleeper.clients.api.role.AssumeSleeperRole;
import sleeper.common.job.action.ActionException;
import sleeper.common.job.action.MessageReference;
import sleeper.common.job.action.thread.PeriodicActionRunnable;
import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.systemtest.configuration.SystemTestDataGenerationJob;
import sleeper.systemtest.configuration.SystemTestDataGenerationJobSerDe;
import sleeper.systemtest.configuration.SystemTestProperties;
import sleeper.systemtest.configuration.SystemTestPropertyValues;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Consumer;

import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_JOBS_QUEUE_URL;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_KEEP_ALIVE_PERIOD_IN_SECONDS;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;

public class ECSSystemTestTask {
    public static final Logger LOGGER = LoggerFactory.getLogger(ECSSystemTestTask.class);
    private final SystemTestPropertyValues properties;
    private final SqsClient sqsClient;
    private final Consumer<SystemTestDataGenerationJob> jobRunner;

    public ECSSystemTestTask(SystemTestPropertyValues properties, SqsClient sqsClient, Consumer<SystemTestDataGenerationJob> jobRunner) {
        this.properties = properties;
        this.sqsClient = sqsClient;
        this.jobRunner = jobRunner;
    }

    public static void main(String[] args) {
        try (StsClient stsClient = StsClient.create();
                SqsClient sqsClient = SqsClient.create()) {
            CommandLineFactory factory = new CommandLineFactory(stsClient, sqsClient);
            if (args.length > 3 || args.length < 2) {
                throw new RuntimeException("Wrong number of arguments detected. Usage: ECSSystemTestTask <standalone-or-combined> <config bucket> <optional role ARN to load combined config as>");
            }
            String deployType = args[0];
            String configBucket = args[1];
            ECSSystemTestTask ingestRandomData;
            if ("standalone".equalsIgnoreCase(deployType)) {
                ingestRandomData = factory.standalone(configBucket);
            } else if (args.length == 3) {
                ingestRandomData = factory.withLoadConfigRole(configBucket, args[2]);
            } else {
                ingestRandomData = factory.noLoadConfigRole(configBucket);
            }
            ingestRandomData.run();
        }
    }

    public void run() {
        String queueUrl = properties.get(SYSTEM_TEST_JOBS_QUEUE_URL);
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(1)
                .waitTimeSeconds(20)
                .build();
        ReceiveMessageResponse receiveMessageResult = sqsClient.receiveMessage(receiveMessageRequest);
        List<Message> messages = receiveMessageResult.messages();
        if (messages.isEmpty()) {
            LOGGER.info("Finishing as no jobs have been received");
            return;
        }
        Message message = messages.get(0);
        LOGGER.info("Received message {}", message.body());
        SystemTestDataGenerationJob job = new SystemTestDataGenerationJobSerDe().fromJson(message.body());

        MessageReference messageReference = new MessageReference(sqsClient, queueUrl,
                "Data generation job " + job.getJobId(), message.receiptHandle());
        // Create background thread to keep messages alive
        PeriodicActionRunnable keepAliveRunnable = new PeriodicActionRunnable(
                messageReference.changeVisibilityTimeoutAction(properties.getInt(SYSTEM_TEST_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS)),
                properties.getInt(SYSTEM_TEST_KEEP_ALIVE_PERIOD_IN_SECONDS));
        keepAliveRunnable.start();
        try {
            jobRunner.accept(job);
            messageReference.deleteAction().call();
        } catch (ActionException e) {
            throw new RuntimeException(e);
        } finally {
            keepAliveRunnable.stop();
        }
    }

    private static class CommandLineFactory {
        private final StsClient stsClient;
        private final SqsClient sqsClient;

        CommandLineFactory(StsClient stsClient, SqsClient sqsClient) {
            this.stsClient = stsClient;
            this.sqsClient = sqsClient;
        }

        ECSSystemTestTask noLoadConfigRole(String configBucket) {
            try (S3Client s3Client = S3Client.builder().build()) {
                return combinedInstance(configBucket, s3Client);
            }
        }

        ECSSystemTestTask withLoadConfigRole(String configBucket, String loadConfigRoleArn) {
            try (S3Client s3Client = AssumeSleeperRole.fromArn(loadConfigRoleArn).forAwsV2(stsClient).buildClient(S3Client.builder())) {
                return combinedInstance(configBucket, s3Client);
            }
        }

        ECSSystemTestTask standalone(String systemTestBucket) {
            try (S3Client s3Client = S3Client.builder().build()) {
                SystemTestStandaloneProperties systemTestProperties = SystemTestStandaloneProperties.fromS3(s3Client, systemTestBucket);
                return new ECSSystemTestTask(systemTestProperties, sqsClient, job -> {
                    try (S3Client instanceS3Client = AssumeSleeperRole.fromArn(job.getRoleArnToLoadConfig()).forAwsV2(stsClient).buildClient(S3Client.builder())) {
                        InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(instanceS3Client, job.getConfigBucket());
                        IngestRandomData ingestData = ingestRandomData(instanceProperties, systemTestProperties);
                        ingestData.run(job);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            }
        }

        ECSSystemTestTask combinedInstance(String configBucket, S3Client s3Client) {
            SystemTestProperties properties = SystemTestProperties.loadFromBucket(s3Client, configBucket);
            IngestRandomData ingestData = ingestRandomData(properties, properties.testPropertiesOnly());
            return new ECSSystemTestTask(properties.testPropertiesOnly(), sqsClient, job -> {
                try {
                    ingestData.run(job);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }

        IngestRandomData ingestRandomData(InstanceProperties instanceProperties, SystemTestPropertyValues systemTestProperties) {
            return new IngestRandomData(instanceProperties, systemTestProperties, stsClient,
                    HadoopConfigurationProvider.getConfigurationForECS(instanceProperties), "/mnt/scratch");
        }
    }
}
