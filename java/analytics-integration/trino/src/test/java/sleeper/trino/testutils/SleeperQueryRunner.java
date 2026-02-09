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
package sleeper.trino.testutils;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.trino.SleeperConfig;
import sleeper.trino.SleeperPlugin;
import sleeper.trino.remotesleeperconnection.HadoopConfigurationProvider;
import sleeper.trino.remotesleeperconnection.HadoopConfigurationProviderForDeployedS3Instance;

import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public final class SleeperQueryRunner {
    private static final String CATALOG = "sleeper";

    private SleeperQueryRunner() {
    }

    public static DistributedQueryRunner createSleeperQueryRunner(
            Map<String, String> extraProperties,
            SleeperConfig sleeperConfig,
            S3Client s3Client,
            S3AsyncClient s3AsyncClient,
            DynamoDbClient dynamoDBClient,
            HadoopConfigurationProvider hadoopConfigurationProvider) throws Exception {
        Session session = testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema("default")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setExtraProperties(extraProperties)
                .build();

        try {
            queryRunner.installPlugin(new SleeperPlugin(new SleeperTestModule(sleeperConfig, s3Client, s3AsyncClient, dynamoDBClient, hadoopConfigurationProvider)));
            queryRunner.createCatalog(CATALOG, "sleeper", ImmutableMap.of());

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());

            return queryRunner;
        } catch (Exception e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    public static void main(String[] args) throws Exception {
        Logging.initialize();

        String configBucket = System.getProperty("sleeper.configbucket");
        requireNonNull(configBucket, "Property sleeper.configbucket must be set");
        SleeperConfig sleeperConfig = new SleeperConfig();
        sleeperConfig.setConfigBucket(configBucket);

        DistributedQueryRunner queryRunner = createSleeperQueryRunner(
                ImmutableMap.of("http-server.http.port", "8080"),
                sleeperConfig,
                S3Client.create(),
                S3AsyncClient.create(),
                DynamoDbClient.create(),
                new HadoopConfigurationProviderForDeployedS3Instance());
        Thread.sleep(10);

        Logger log = Logger.get(SleeperQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
