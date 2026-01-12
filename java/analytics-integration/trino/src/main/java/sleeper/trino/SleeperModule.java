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
package sleeper.trino;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.trino.remotesleeperconnection.HadoopConfigurationProvider;
import sleeper.trino.remotesleeperconnection.HadoopConfigurationProviderForDeployedS3Instance;
import sleeper.trino.remotesleeperconnection.SleeperConnectionAsTrino;

import static io.airlift.configuration.ConfigBinder.configBinder;

/**
 * The Guice module configuration. This is used by Guice to create singleton classes and bind the sleeper.properties
 * file to the {@link sleeper.trino.SleeperConfig} class.
 */
public class SleeperModule implements Module {
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

        // The connections to AWS services currently use the default connection only, which is convenient as it
        // allows the user to run an 'aws configure' command before starting Trino and the configured connection
        // will be used. It would be possible to allow a choice of connection via the connector config file.
        // According to the AWS documentation, all Java clients are thread-safe and they encourage their use in
        // multiple threads.
        //
        // A more bespoke S3AsyncClient may be created as follows:
        //        S3AsyncClient s3AsyncClient = S3AsyncClient.builder()
        //                .httpClientBuilder(NettyNioAsyncHttpClient.builder().maxConcurrency(200))
        //                .build();
        //        binder.bind(S3AsyncClient.class).toInstance(s3AsyncClient);
        // This does not appear to offer any performance improvements. Note that an additional Maven dependency
        // is required to support this.
        binder.bind(S3Client.class).toInstance(S3Client.create());
        binder.bind(S3AsyncClient.class).toInstance(S3AsyncClient.create());
        binder.bind(DynamoDbClient.class).toInstance(DynamoDbClient.create());

        // Bind the Hadoop configuration provider which is used when reading and writing Parquet files to S3
        binder.bind(HadoopConfigurationProvider.class).toInstance(new HadoopConfigurationProviderForDeployedS3Instance());

        // Parse the sleeper.properties config file, populate the SleeperConfig object and make it available for injection
        configBinder(binder).bindConfig(SleeperConfig.class);
    }
}
