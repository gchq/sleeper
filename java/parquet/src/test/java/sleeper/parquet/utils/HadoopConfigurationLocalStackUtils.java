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
package sleeper.parquet.utils;

import org.apache.hadoop.conf.Configuration;
import org.testcontainers.containers.localstack.LocalStackContainer;

public class HadoopConfigurationLocalStackUtils {

    private HadoopConfigurationLocalStackUtils() {
    }

    /**
     * Only use one Hadoop configuration from here at once if your code uses the Hadoop file system, for example if you
     * write Parquet files to S3A. The Hadoop FileSystem caches the S3AFileSystem objects which actually communicate
     * with S3 and this means that any new LocalStack container will not be recognised once the first one has been used.
     * The FileSystem cache needs to be reset between different uses of a LocalStack container.
     *
     * @param  container LocalStack container to access for S3A file system
     * @return           Hadoop configuration
     */
    public static Configuration getHadoopConfiguration(LocalStackContainer container) {
        Configuration configuration = new Configuration();
        configureHadoop(configuration, container);
        return configuration;
    }

    public static void configureHadoop(Configuration configuration, LocalStackContainer container) {
        configuration.setClassLoader(HadoopConfigurationLocalStackUtils.class.getClassLoader());
        configuration.set("fs.s3a.endpoint", container.getEndpointOverride(LocalStackContainer.Service.S3).toString());
        configuration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        configuration.set("fs.s3a.access.key", container.getAccessKey());
        configuration.set("fs.s3a.secret.key", container.getSecretKey());
        configuration.setInt("fs.s3a.connection.maximum", 25);
        configuration.setBoolean("fs.s3a.connection.ssl.enabled", false);
        // The following settings may be useful if the connection to the localstack S3 instance hangs.
        // These settings attempt to force connection issues to generate errors early.
        // The settings do help but errors may still take many minutes to appear.
        // configuration.set("fs.s3a.connection.timeout", "1000");
        // configuration.set("fs.s3a.connection.establish.timeout", "1");
        // configuration.set("fs.s3a.attempts.maximum", "1");
    }
}
