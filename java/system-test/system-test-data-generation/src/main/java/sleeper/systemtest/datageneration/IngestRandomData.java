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
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private final AWSSecurityTokenService stsClientV1;
    private final StsClient stsClientV2;
    private final Configuration hadoopConf;
    private final String localDir;

    public IngestRandomData(
            InstanceProperties instanceProperties, SystemTestPropertyValues systemTestProperties,
            AWSSecurityTokenService stsClientV1, StsClient stsClientV2, Configuration hadoopConf, String localDir) {
        this.instanceProperties = instanceProperties;
        this.systemTestProperties = systemTestProperties;
        this.stsClientV1 = stsClientV1;
        this.stsClientV2 = stsClientV2;
        this.hadoopConf = hadoopConf;
        this.localDir = localDir;
    }

    public void run(String tableName) throws IOException {
        run(SystemTestDataGenerationJob.builder()
                .tableName(tableName)
                .properties(systemTestProperties)
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
        AWSSecurityTokenService stsClientV1 = AWSSecurityTokenServiceClientBuilder.defaultClient();
        try (StsClient stsClientV2 = StsClient.create()) {
            CommandLineFactory factory = new CommandLineFactory(stsClientV1, stsClientV2);
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
        } finally {
            stsClientV1.shutdown();
        }
    }

    private Ingester ingester(SystemTestDataGenerationJob job) {
        SystemTestIngestMode ingestMode = job.getIngestMode();
        if (ingestMode == DIRECT) {
            return () -> {
                try (InstanceIngestSession session = InstanceIngestSession.direct(stsClientV1, stsClientV2, instanceProperties, job.getTableName(), localDir)) {
                    WriteRandomDataDirect.writeWithIngestFactory(job, session);
                }
            };
        }
        return () -> {
            try (InstanceIngestSession session = InstanceIngestSession.byQueue(stsClientV1, stsClientV2, instanceProperties, job.getTableName(), localDir)) {
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
        private final AWSSecurityTokenService stsClientV1;
        private final StsClient stsClientV2;

        CommandLineFactory(AWSSecurityTokenService stsClientV1, StsClient stsClientV2) {
            this.stsClientV1 = stsClientV1;
            this.stsClientV2 = stsClientV2;
        }

        IngestRandomData noLoadConfigRole(String configBucket) {
            AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
            try {
                return combinedInstance(configBucket, s3Client);
            } finally {
                s3Client.shutdown();
            }
        }

        IngestRandomData withLoadConfigRole(String configBucket, String loadConfigRoleArn) {
            AmazonS3 instanceS3Client = AssumeSleeperRole.fromArn(loadConfigRoleArn).forAwsV1(stsClientV1).buildClient(AmazonS3ClientBuilder.standard());
            try {
                return combinedInstance(configBucket, instanceS3Client);
            } finally {
                instanceS3Client.shutdown();
            }
        }

        IngestRandomData standalone(String configBucket, String loadConfigRoleArn, String systemTestBucket) {
            AmazonS3 instanceS3Client = AssumeSleeperRole.fromArn(loadConfigRoleArn).forAwsV1(stsClientV1).buildClient(AmazonS3ClientBuilder.standard());
            AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
            try {
                SystemTestStandaloneProperties systemTestProperties = SystemTestStandaloneProperties.fromS3(s3Client, systemTestBucket);
                InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(instanceS3Client, configBucket);
                return ingestRandomData(instanceProperties, systemTestProperties);
            } finally {
                s3Client.shutdown();
                instanceS3Client.shutdown();
            }
        }

        IngestRandomData combinedInstance(String configBucket, AmazonS3 s3Client) {
            SystemTestProperties properties = SystemTestProperties.loadFromBucket(s3Client, configBucket);
            return ingestRandomData(properties, properties.testPropertiesOnly());
        }

        IngestRandomData ingestRandomData(InstanceProperties instanceProperties, SystemTestPropertyValues systemTestProperties) {
            return new IngestRandomData(instanceProperties, systemTestProperties, stsClientV1, stsClientV2,
                    HadoopConfigurationProvider.getConfigurationForECS(instanceProperties), "/mnt/scratch");
        }
    }
}
