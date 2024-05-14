/*
 * Copyright 2022-2024 Crown Copyright
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.util.AssumeSleeperRole;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.systemtest.configuration.SystemTestIngestMode;
import sleeper.systemtest.configuration.SystemTestProperties;
import sleeper.systemtest.configuration.SystemTestPropertyValues;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;

import java.io.IOException;
import java.util.UUID;

import static sleeper.systemtest.configuration.SystemTestIngestMode.BATCHER;
import static sleeper.systemtest.configuration.SystemTestIngestMode.DIRECT;
import static sleeper.systemtest.configuration.SystemTestIngestMode.GENERATE_ONLY;
import static sleeper.systemtest.configuration.SystemTestIngestMode.QUEUE;
import static sleeper.systemtest.configuration.SystemTestProperty.INGEST_MODE;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_INGESTS_PER_WRITER;

/**
 * Entrypoint for SystemTest image. Writes random data to Sleeper using the mechanism (ingestMode) defined in
 * the properties which were written to S3.
 */
public class IngestRandomData {

    private static final Logger LOGGER = LoggerFactory.getLogger(IngestRandomData.class);

    private final InstanceProperties instanceProperties;
    private final SystemTestPropertyValues systemTestProperties;
    private final String tableName;
    private final AWSSecurityTokenService stsClient;

    private IngestRandomData(
            InstanceProperties instanceProperties, SystemTestPropertyValues systemTestProperties, String tableName,
            AWSSecurityTokenService stsClient) {
        this.instanceProperties = instanceProperties;
        this.systemTestProperties = systemTestProperties;
        this.tableName = tableName;
        this.stsClient = stsClient;
    }

    public void run() throws IOException {
        Ingester ingester = ingester();
        int numIngests = systemTestProperties.getInt(NUMBER_OF_INGESTS_PER_WRITER);
        for (int i = 1; i <= numIngests; i++) {
            LOGGER.info("Starting ingest {}", i);
            ingester.ingest();
            LOGGER.info("Completed ingest {}", i);
        }
        LOGGER.info("Finished");
    }

    public static void main(String[] args) throws IOException {
        AWSSecurityTokenService stsClient = AWSSecurityTokenServiceClientBuilder.defaultClient();
        try {
            CommandLineFactory factory = new CommandLineFactory(stsClient);
            IngestRandomData ingestRandomData;
            if (args.length == 4) {
                ingestRandomData = factory.standalone(args[0], args[1], args[2], args[3]);
            } else if (args.length == 3) {
                ingestRandomData = factory.withLoadConfigRole(args[0], args[1], args[2]);
            } else if (args.length == 2) {
                ingestRandomData = factory.noLoadConfigRole(args[0], args[1]);
            } else {
                throw new RuntimeException("Wrong number of arguments detected. Usage: IngestRandomData <S3 bucket> <Table name> <optional role ARN to load config as> <optional system test bucket>");
            }

            ingestRandomData.run();
        } finally {
            stsClient.shutdown();
        }
    }

    private Ingester ingester() {
        SystemTestIngestMode ingestMode = systemTestProperties.getEnumValue(INGEST_MODE, SystemTestIngestMode.class);
        if (ingestMode == DIRECT) {
            return () -> {
                AssumeSleeperRole assumeRole = AssumeSleeperRole.directIngest(stsClient, instanceProperties);
                try (InstanceIngestSession session = new InstanceIngestSession(assumeRole, instanceProperties, tableName)) {
                    WriteRandomDataDirect.writeWithIngestFactory(systemTestProperties, session);
                }
            };
        }
        return () -> {
            AssumeSleeperRole assumeRole = AssumeSleeperRole.ingestByQueue(stsClient, instanceProperties);
            try (InstanceIngestSession session = new InstanceIngestSession(assumeRole, instanceProperties, tableName)) {
                String jobId = UUID.randomUUID().toString();
                String dir = WriteRandomDataFiles.writeToS3GetDirectory(systemTestProperties, session, jobId);

                if (ingestMode == QUEUE) {
                    IngestRandomDataViaQueue.sendJob(jobId, dir, systemTestProperties, session);
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
        private final AWSSecurityTokenService stsClient;

        CommandLineFactory(AWSSecurityTokenService stsClient) {
            this.stsClient = stsClient;
        }

        IngestRandomData noLoadConfigRole(String configBucket, String tableName) {
            AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
            try {
                return combinedInstance(configBucket, tableName, s3Client);
            } finally {
                s3Client.shutdown();
            }
        }

        IngestRandomData withLoadConfigRole(String configBucket, String tableName, String loadConfigRoleArn) {
            AmazonS3 s3Client = AssumeSleeperRole.fromArn(stsClient, loadConfigRoleArn).v1Client(AmazonS3ClientBuilder.standard());
            try {
                return combinedInstance(configBucket, tableName, s3Client);
            } finally {
                s3Client.shutdown();
            }
        }

        IngestRandomData standalone(String configBucket, String tableName, String loadConfigRoleArn, String systemTestBucket) {
            AmazonS3 instanceS3Client = AssumeSleeperRole.fromArn(stsClient, loadConfigRoleArn).v1Client(AmazonS3ClientBuilder.standard());
            AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
            try {
                SystemTestStandaloneProperties systemTestProperties = SystemTestStandaloneProperties.fromS3(s3Client, systemTestBucket);
                InstanceProperties instanceProperties = new InstanceProperties();
                instanceProperties.loadFromS3(instanceS3Client, configBucket);
                return new IngestRandomData(instanceProperties, systemTestProperties, tableName, stsClient);
            } finally {
                s3Client.shutdown();
                instanceS3Client.shutdown();
            }
        }

        IngestRandomData combinedInstance(String configBucket, String tableName, AmazonS3 s3Client) {
            SystemTestProperties properties = new SystemTestProperties();
            properties.loadFromS3(s3Client, configBucket);
            return new IngestRandomData(properties, properties.testPropertiesOnly(), tableName, stsClient);
        }
    }
}
