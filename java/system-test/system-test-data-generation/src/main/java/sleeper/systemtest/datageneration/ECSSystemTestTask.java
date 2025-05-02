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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sts.StsClient;

import sleeper.clients.util.role.AssumeSleeperRole;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.job.common.action.ActionException;
import sleeper.job.common.action.MessageReference;
import sleeper.job.common.action.thread.PeriodicActionRunnable;
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
    private final AmazonSQS sqsClient;
    private final Consumer<SystemTestDataGenerationJob> jobRunner;

    public ECSSystemTestTask(SystemTestPropertyValues properties, AmazonSQS sqsClient, Consumer<SystemTestDataGenerationJob> jobRunner) {
        this.properties = properties;
        this.sqsClient = sqsClient;
        this.jobRunner = jobRunner;
    }

    public static void main(String[] args) {
        AWSSecurityTokenService stsClientV1 = AWSSecurityTokenServiceClientBuilder.defaultClient();
        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
        try (StsClient stsClientV2 = StsClient.create()) {
            CommandLineFactory factory = new CommandLineFactory(stsClientV1, stsClientV2, sqsClient);
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
        } finally {
            stsClientV1.shutdown();
            sqsClient.shutdown();
        }
    }

    public void run() {
        String queueUrl = properties.get(SYSTEM_TEST_JOBS_QUEUE_URL);
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl)
                .withMaxNumberOfMessages(1)
                .withWaitTimeSeconds(20);
        ReceiveMessageResult receiveMessageResult = sqsClient.receiveMessage(receiveMessageRequest);
        List<Message> messages = receiveMessageResult.getMessages();
        if (messages.isEmpty()) {
            LOGGER.info("Finishing as no jobs have been received");
            return;
        }
        Message message = messages.get(0);
        LOGGER.info("Received message {}", message.getBody());
        SystemTestDataGenerationJob job = new SystemTestDataGenerationJobSerDe().fromJson(message.getBody());

        MessageReference messageReference = new MessageReference(sqsClient, queueUrl,
                "Data generation job " + job.getJobId(), message.getReceiptHandle());
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
        private final AWSSecurityTokenService stsClientV1;
        private final StsClient stsClientV2;
        private final AmazonSQS sqsClient;

        CommandLineFactory(AWSSecurityTokenService stsClientV1, StsClient stsClientV2, AmazonSQS sqsClient) {
            this.stsClientV1 = stsClientV1;
            this.stsClientV2 = stsClientV2;
            this.sqsClient = sqsClient;
        }

        ECSSystemTestTask noLoadConfigRole(String configBucket) {
            AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
            try {
                return combinedInstance(configBucket, s3Client);
            } finally {
                s3Client.shutdown();
            }
        }

        ECSSystemTestTask withLoadConfigRole(String configBucket, String loadConfigRoleArn) {
            AmazonS3 s3Client = AssumeSleeperRole.fromArn(loadConfigRoleArn).forAwsV1(stsClientV1).buildClient(AmazonS3ClientBuilder.standard());
            try {
                return combinedInstance(configBucket, s3Client);
            } finally {
                s3Client.shutdown();
            }
        }

        ECSSystemTestTask standalone(String systemTestBucket) {
            AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
            try {
                SystemTestStandaloneProperties systemTestProperties = SystemTestStandaloneProperties.fromS3(s3Client, systemTestBucket);
                return new ECSSystemTestTask(systemTestProperties, sqsClient, job -> {
                    AmazonS3 instanceS3Client = AssumeSleeperRole.fromArn(job.getRoleArnToLoadConfig()).forAwsV1(stsClientV1).buildClient(AmazonS3ClientBuilder.standard());
                    try {
                        InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(instanceS3Client, job.getConfigBucket());
                        IngestRandomData ingestData = ingestRandomData(instanceProperties, systemTestProperties);
                        ingestData.run(job);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    } finally {
                        instanceS3Client.shutdown();
                    }
                });
            } finally {
                s3Client.shutdown();
            }
        }

        ECSSystemTestTask combinedInstance(String configBucket, AmazonS3 s3Client) {
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
            return new IngestRandomData(instanceProperties, systemTestProperties, stsClientV1, stsClientV2,
                    HadoopConfigurationProvider.getConfigurationForECS(instanceProperties), "/mnt/scratch");
        }
    }
}
