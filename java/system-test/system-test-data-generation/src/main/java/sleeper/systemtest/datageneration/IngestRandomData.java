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

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;

import sleeper.clients.api.role.AssumeSleeperRole;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.systemtest.configuration.SystemTestDataGenerationJob;
import sleeper.systemtest.configuration.SystemTestIngestMode;
import sleeper.systemtest.configuration.SystemTestProperties;
import sleeper.systemtest.configuration.SystemTestPropertyValues;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;

import java.io.IOException;

import static sleeper.systemtest.configuration.SystemTestIngestMode.BATCHER;
import static sleeper.systemtest.configuration.SystemTestIngestMode.DIRECT;
import static sleeper.systemtest.configuration.SystemTestIngestMode.GENERATE_ONLY;
import static sleeper.systemtest.configuration.SystemTestIngestMode.QUEUE;

/**
 * Entrypoint for SystemTest image. Writes random data to Sleeper using the mechanism (ingestMode) defined in
 * the properties which were written to S3.
 */
public class IngestRandomData {

    private static final Logger LOGGER = LoggerFactory.getLogger(IngestRandomData.class);

    private final InstanceProperties instanceProperties;
    private final SystemTestPropertyValues systemTestProperties;
    private final StsClient stsClient;
    private final Configuration hadoopConf;
    private final String localDir;

    public IngestRandomData(
            InstanceProperties instanceProperties, SystemTestPropertyValues systemTestProperties,
            StsClient stsClient, Configuration hadoopConf, String localDir) {
        this.instanceProperties = instanceProperties;
        this.systemTestProperties = systemTestProperties;
        this.stsClient = stsClient;
        this.hadoopConf = hadoopConf;
        this.localDir = localDir;
    }

    public void run(String tableName) throws IOException {
        run(SystemTestDataGenerationJob.builder()
                .instanceProperties(instanceProperties)
                .testProperties(systemTestProperties)
                .tableName(tableName)
                .build());
    }

    public void run(SystemTestDataGenerationJob job) throws IOException {
        Ingester ingester = ingester(job);
        for (int i = 1; i <= job.getNumberOfIngests(); i++) {
            LOGGER.info("Starting ingest {}", i);
            ingester.ingest();
            LOGGER.info("Completed ingest {}", i);
        }
        LOGGER.info("Finished");
    }

    public static void main(String[] args) throws IOException {
        try (StsClient stsClient = StsClient.create()) {
            CommandLineFactory factory = new CommandLineFactory(stsClient);
            IngestRandomData ingestRandomData;
            String s3Bucket = args[0];
            String tableName = args[1];
            if (args.length == 4) {
                ingestRandomData = factory.standalone(s3Bucket, args[2], args[3]);
            } else if (args.length == 3) {
                ingestRandomData = factory.withLoadConfigRole(s3Bucket, args[2]);
            } else if (args.length == 2) {
                ingestRandomData = factory.noLoadConfigRole(s3Bucket);
            } else {
                throw new RuntimeException("Wrong number of arguments detected. Usage: IngestRandomData <S3 bucket> <Table name> <optional role ARN to load config as> <optional system test bucket>");
            }

            ingestRandomData.run(tableName);
        }
    }

    private Ingester ingester(SystemTestDataGenerationJob job) {
        SystemTestIngestMode ingestMode = job.getIngestMode();
        if (ingestMode == DIRECT) {
            return () -> {
                try (InstanceIngestSession session = InstanceIngestSession.direct(stsClient, instanceProperties, job.getTableName(), localDir)) {
                    WriteRandomDataDirect.writeWithIngestFactory(job, session);
                }
            };
        }
        return () -> {
            try (InstanceIngestSession session = InstanceIngestSession.byQueue(stsClient, instanceProperties, job.getTableName(), localDir)) {
                String dir = WriteRandomDataFiles.writeToS3GetDirectory(systemTestProperties, session.tableProperties(), hadoopConf, job);

                if (ingestMode == QUEUE) {
                    IngestRandomDataViaQueue.sendJob(job.getJobId(), dir, job, session);
                } else if (ingestMode == BATCHER) {
                    IngestRandomDataViaBatcher.sendRequest(dir, session);
                } else if (ingestMode == GENERATE_ONLY) {
                    LOGGER.debug("Generate data only, no message was sent");
                } else {
                    throw new IllegalArgumentException("Unrecognised ingest mode: " + ingestMode);
                }
            }
        };
    }

    interface Ingester {
        void ingest() throws IOException;
    }

    private static class CommandLineFactory {
        private final StsClient stsClient;

        CommandLineFactory(StsClient stsClient) {
            this.stsClient = stsClient;
        }

        IngestRandomData noLoadConfigRole(String configBucket) {
            try (S3Client s3Client = S3Client.builder().build()) {
                return combinedInstance(configBucket, s3Client);
            }
        }

        IngestRandomData withLoadConfigRole(String configBucket, String loadConfigRoleArn) {
            try (S3Client instanceS3Client = AssumeSleeperRole.fromArn(loadConfigRoleArn).forAwsSdk(stsClient).buildClient(S3Client.builder())) {
                return combinedInstance(configBucket, instanceS3Client);
            }
        }

        IngestRandomData standalone(String configBucket, String loadConfigRoleArn, String systemTestBucket) {
            try (S3Client instanceS3Client = AssumeSleeperRole.fromArn(loadConfigRoleArn).forAwsSdk(stsClient).buildClient(S3Client.builder());
                    S3Client s3Client = S3Client.builder().build()) {
                SystemTestStandaloneProperties systemTestProperties = SystemTestStandaloneProperties.fromS3(s3Client, systemTestBucket);
                InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(instanceS3Client, configBucket);
                return ingestRandomData(instanceProperties, systemTestProperties);
            }
        }

        IngestRandomData combinedInstance(String configBucket, S3Client s3Client) {
            SystemTestProperties properties = SystemTestProperties.loadFromBucket(s3Client, configBucket);
            return ingestRandomData(properties, properties.testPropertiesOnly());
        }

        IngestRandomData ingestRandomData(InstanceProperties instanceProperties, SystemTestPropertyValues systemTestProperties) {
            return new IngestRandomData(instanceProperties, systemTestProperties, stsClient,
                    HadoopConfigurationProvider.getConfigurationForECS(instanceProperties), "/mnt/scratch");
        }
    }
}
