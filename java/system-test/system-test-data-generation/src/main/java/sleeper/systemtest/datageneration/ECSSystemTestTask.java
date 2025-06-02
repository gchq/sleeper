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

import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sts.StsClient;

import sleeper.clients.api.role.AssumeSleeperRole;
import sleeper.clients.api.role.AssumeSleeperRoleV2;
import sleeper.common.jobv2.action.ActionException;
import sleeper.common.jobv2.action.MessageReference;
import sleeper.common.jobv2.action.thread.PeriodicActionRunnable;
import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.systemtest.configurationv2.SystemTestDataGenerationJob;
import sleeper.systemtest.configurationv2.SystemTestDataGenerationJobSerDe;
import sleeper.systemtest.configurationv2.SystemTestProperties;
import sleeper.systemtest.configurationv2.SystemTestPropertyValues;
import sleeper.systemtest.configurationv2.SystemTestStandaloneProperties;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Consumer;

import static sleeper.systemtest.configurationv2.SystemTestProperty.SYSTEM_TEST_JOBS_QUEUE_URL;
import static sleeper.systemtest.configurationv2.SystemTestProperty.SYSTEM_TEST_KEEP_ALIVE_PERIOD_IN_SECONDS;
import static sleeper.systemtest.configurationv2.SystemTestProperty.SYSTEM_TEST_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;

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
        AWSSecurityTokenService stsClientV1 = AWSSecurityTokenServiceClientBuilder.defaultClient();
        try (StsClient stsClientV2 = StsClient.create(); SqsClient sqsClientV2 = SqsClient.create()) {
            CommandLineFactory factory = new CommandLineFactory(stsClientV1, stsClientV2, sqsClientV2);
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
        private final AWSSecurityTokenService stsClientV1;
        private final StsClient stsClientV2;
        private final SqsClient sqsClientV2;

        CommandLineFactory(AWSSecurityTokenService stsClientV1, StsClient stsClientV2, SqsClient sqsClientV2) {
            this.stsClientV1 = stsClientV1;
            this.stsClientV2 = stsClientV2;
            this.sqsClientV2 = sqsClientV2;
        }

        ECSSystemTestTask noLoadConfigRole(String configBucket) {
            S3Client s3Client = S3Client.builder().build();
            try {
                return combinedInstance(configBucket, s3Client);
            } finally {
                s3Client.close();
            }
        }

        ECSSystemTestTask withLoadConfigRole(String configBucket, String loadConfigRoleArn) {
            AssumeSleeperRole assumeRole = AssumeSleeperRole.instanceAdmin(this.properties);
            AssumeSleeperRoleV2 v2 = assumeRole.forAwsV2(StsClient.create());
            S3Client s3Client = v2.buildClient(S3Client.builder());
            try {
                return combinedInstance(configBucket, s3Client);
            } finally {
                s3Client.close();
            }
        }

        ECSSystemTestTask standalone(String systemTestBucket) {
            S3Client s3Client = S3Client.builder().build();
            try {
                SystemTestStandaloneProperties systemTestProperties = SystemTestStandaloneProperties.fromS3(s3Client, systemTestBucket);
                return new ECSSystemTestTask(systemTestProperties, sqsClientV2, job -> {
                    AssumeSleeperRoleV2 v2 = AssumeSleeperRole.forAwsV2(stsClientV2);
                    S3Client instanceS3Client = v2.buildClient(S3Client.builder());
                    try {
                        InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(instanceS3Client, job.getConfigBucket());
                        IngestRandomData ingestData = ingestRandomData(instanceProperties, systemTestProperties);
                        ingestData.run(job);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    } finally {
                        instanceS3Client.close();
                    }
                });
            } finally {
                s3Client.close();
            }
        }

        ECSSystemTestTask combinedInstance(String configBucket, S3Client s3Client) {
            SystemTestProperties properties = SystemTestProperties.loadFromBucket(s3Client, configBucket);
            IngestRandomData ingestData = ingestRandomData(properties, properties.testPropertiesOnly());
            return new ECSSystemTestTask(properties.testPropertiesOnly(), sqsClientV2, job -> {
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
