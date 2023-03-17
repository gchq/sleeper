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
package sleeper.trino.testutils;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import sleeper.trino.SleeperConfig;
import sleeper.trino.SleeperConnector;
import sleeper.trino.SleeperMetadata;
import sleeper.trino.SleeperPageSinkProvider;
import sleeper.trino.SleeperRecordSetProvider;
import sleeper.trino.SleeperSplitManager;
import sleeper.trino.remotesleeperconnection.HadoopConfigurationProvider;
import sleeper.trino.remotesleeperconnection.SleeperConnectionAsTrino;

import static java.util.Objects.requireNonNull;

/**
 * The Guice module configuration, for use during testing. This is used by Guice to create singleton classes.
 */
public class SleeperTestModule implements Module {
    private final SleeperConfig sleeperConfig;
    private final AmazonS3 s3Client;
    private final S3AsyncClient s3AsyncClient;
    private final AmazonDynamoDB dynamoDbClient;
    private final HadoopConfigurationProvider hadoopConfigurationProvider;

    public SleeperTestModule(SleeperConfig sleeperConfig,
                             AmazonS3 s3Client,
                             S3AsyncClient s3AsyncClient,
                             AmazonDynamoDB dynamoDbClient,
                             HadoopConfigurationProvider hadoopConfigurationProvider) {
        this.sleeperConfig = requireNonNull(sleeperConfig);
        this.s3Client = requireNonNull(s3Client);
        this.s3AsyncClient = requireNonNull(s3AsyncClient);
        this.dynamoDbClient = requireNonNull(dynamoDbClient);
        this.hadoopConfigurationProvider = requireNonNull(hadoopConfigurationProvider);
    }

    @Override
    public void configure(Binder binder) {
        // Insist on very strict interpretation of Guice rules to avoid any unexpected class injection
        binder.requireExplicitBindings();
        binder.requireExactBindingAnnotations();
        binder.requireAtInjectOnConstructors();
        binder.disableCircularProxies();

        // Set up several singleton classes which are available wherever Guice injects constructor arguments
        binder.bind(SleeperConnector.class).in(Scopes.SINGLETON);
        binder.bind(SleeperMetadata.class).in(Scopes.SINGLETON);
        binder.bind(SleeperSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(SleeperRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(SleeperPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(SleeperConnectionAsTrino.class).in(Scopes.SINGLETON);

        // Bind the supplied instances
        binder.bind(SleeperConfig.class).toInstance(sleeperConfig);
        binder.bind(AmazonS3.class).toInstance(s3Client);
        binder.bind(S3AsyncClient.class).toInstance(s3AsyncClient);
        binder.bind(AmazonDynamoDB.class).toInstance(dynamoDbClient);
        binder.bind(HadoopConfigurationProvider.class).toInstance(hadoopConfigurationProvider);
    }
}
