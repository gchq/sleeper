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
import software.amazon.awssdk.services.sts.StsClient;

import sleeper.clients.api.role.AssumeSleeperRole;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.systemtest.configuration.SystemTestDataGenerationJob;
import sleeper.systemtest.configuration.SystemTestDataGenerationJobStore;
import sleeper.systemtest.configuration.SystemTestProperties;
import sleeper.systemtest.configuration.SystemTestPropertyValues;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Consumer;

public class ECSSystemTestTask {
    public static final Logger LOGGER = LoggerFactory.getLogger(ECSSystemTestTask.class);

    private final SystemTestDataGenerationJobStore jobStore;
    private final String jobObjectKey;
    private final Consumer<SystemTestDataGenerationJob> jobRunner;

    public ECSSystemTestTask(SystemTestPropertyValues properties, S3Client s3Client, String jobObjectKey, Consumer<SystemTestDataGenerationJob> jobRunner) {
        this.jobStore = new SystemTestDataGenerationJobStore(properties, s3Client);
        this.jobObjectKey = jobObjectKey;
        this.jobRunner = jobRunner;
    }

    public static void main(String[] args) {
        try (StsClient stsClient = StsClient.create();
                S3Client s3Client = S3Client.create()) {
            CommandLineFactory factory = new CommandLineFactory(stsClient, s3Client);
            if (args.length > 4 || args.length < 3) {
                throw new RuntimeException(
                        "Wrong number of arguments detected. Usage: ECSSystemTestTask <standalone-or-combined> <job-object-key> <config bucket> <optional role ARN to load combined config as>");
            }
            String deployType = args[0];
            String jobObjectKey = args[1];
            String configBucket = args[2];
            ECSSystemTestTask ingestRandomData;
            if ("standalone".equalsIgnoreCase(deployType)) {
                ingestRandomData = factory.standalone(jobObjectKey, configBucket);
            } else if (args.length == 4) {
                ingestRandomData = factory.withLoadConfigRole(jobObjectKey, configBucket, args[3]);
            } else {
                ingestRandomData = factory.noLoadConfigRole(jobObjectKey, configBucket);
            }
            ingestRandomData.run();
        }
    }

    public void run() {
        SystemTestDataGenerationJob job = jobStore.readJob(jobObjectKey);
        LOGGER.info("Running job: {}", job);
        jobRunner.accept(job);
    }

    private static class CommandLineFactory {
        private final StsClient stsClient;
        private final S3Client s3Client;

        CommandLineFactory(StsClient stsClient, S3Client s3Client) {
            this.stsClient = stsClient;
            this.s3Client = s3Client;
        }

        ECSSystemTestTask noLoadConfigRole(String jobObjectKey, String configBucket) {
            return combinedInstance(jobObjectKey, configBucket, s3Client);
        }

        ECSSystemTestTask withLoadConfigRole(String jobObjectKey, String configBucket, String loadConfigRoleArn) {
            try (S3Client loadConfigS3Client = AssumeSleeperRole.fromArn(loadConfigRoleArn).forAwsSdk(stsClient).buildClient(S3Client.builder())) {
                return combinedInstance(jobObjectKey, configBucket, loadConfigS3Client);
            }
        }

        ECSSystemTestTask standalone(String jobObjectKey, String systemTestBucket) {
            SystemTestStandaloneProperties systemTestProperties = SystemTestStandaloneProperties.fromS3(s3Client, systemTestBucket);
            return new ECSSystemTestTask(systemTestProperties, s3Client, jobObjectKey, job -> {
                try (S3Client instanceS3Client = AssumeSleeperRole.fromArn(job.getRoleArnToLoadConfig()).forAwsSdk(stsClient).buildClient(S3Client.builder())) {
                    InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(instanceS3Client, job.getConfigBucket());
                    IngestRandomData ingestData = ingestRandomData(instanceProperties, systemTestProperties);
                    ingestData.run(job);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }

        ECSSystemTestTask combinedInstance(String jobObjectKey, String configBucket, S3Client loadConfigS3Client) {
            SystemTestProperties properties = SystemTestProperties.loadFromBucket(loadConfigS3Client, configBucket);
            IngestRandomData ingestData = ingestRandomData(properties, properties.testPropertiesOnly());
            return new ECSSystemTestTask(properties.testPropertiesOnly(), s3Client, jobObjectKey, job -> {
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
