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
package sleeper.trino.testutils;

import org.apache.hadoop.conf.Configuration;
import org.testcontainers.containers.localstack.LocalStackContainer;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.trino.remotesleeperconnection.HadoopConfigurationProvider;

import static java.util.Objects.requireNonNull;

public class HadoopConfigurationProviderForLocalStack implements HadoopConfigurationProvider {
    private final LocalStackContainer localStackContainer;

    public HadoopConfigurationProviderForLocalStack(LocalStackContainer localStackContainer) {
        this.localStackContainer = requireNonNull(localStackContainer);
    }

    @Override
    public Configuration getHadoopConfiguration(InstanceProperties instanceProperties) {
        Configuration configuration = new Configuration();
        configuration.setClassLoader(this.getClass().getClassLoader());
        configuration.set("fs.s3a.endpoint", localStackContainer.getEndpointOverride(LocalStackContainer.Service.S3).toString());
        configuration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        configuration.set("fs.s3a.access.key", localStackContainer.getAccessKey());
        configuration.set("fs.s3a.secret.key", localStackContainer.getSecretKey());
        configuration.setBoolean("fs.s3a.connection.ssl.enabled", false);
        // The following settings may be useful if the connection to the localstack S3 instance hangs.
        // These settings attempt to force connection issues to generate errors ealy.
        // The settings do help but errors mayn still take many minutes to appear.
        // configuration.set("fs.s3a.connection.timeout", "1000");
        // configuration.set("fs.s3a.connection.establish.timeout", "1");
        // configuration.set("fs.s3a.attempts.maximum", "1");
        return configuration;
    }
}
