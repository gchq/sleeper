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
package sleeper.localstack.test;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import org.apache.hadoop.conf.Configuration;

public class WiremockHadoopConfigurationProvider {

    private WiremockHadoopConfigurationProvider() {
    }

    /**
     * Only use one Hadoop configuration from here at once if your code uses the Hadoop file system, for example if you
     * write Parquet files to S3A. The Hadoop FileSystem caches the S3AFileSystem objects which actually communicate
     * with S3 and this means that any new Wiremock runtime will not be recognised once the first one has been used.
     * The FileSystem cache needs to be reset between different uses of a Wiremock runtime.
     *
     * @param  runtimeInfo Wiremock runtime to mock access for S3A file system
     * @return             Hadoop configuration
     */
    public static Configuration getHadoopConfiguration(WireMockRuntimeInfo runtimeInfo) {
        Configuration configuration = new Configuration();
        configureHadoop(configuration, runtimeInfo);
        return configuration;
    }

    public static void configureHadoop(Configuration configuration, WireMockRuntimeInfo runtimeInfo) {
        configuration.setClassLoader(WiremockHadoopConfigurationProvider.class.getClassLoader());
        configuration.set("fs.s3a.endpoint", runtimeInfo.getHttpBaseUrl());
        configuration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        configuration.set("fs.s3a.access.key", WiremockAwsV2ClientHelper.WIREMOCK_ACCESS_KEY);
        configuration.set("fs.s3a.secret.key", WiremockAwsV2ClientHelper.WIREMOCK_SECRET_KEY);
        configuration.setInt("fs.s3a.connection.maximum", 25);
        configuration.setBoolean("fs.s3a.connection.ssl.enabled", false);
        configuration.setBoolean("fs.s3a.path.style.access", true);
        // The following settings may be useful if the connection to the Wiremock S3 instance hangs.
        // These settings attempt to force connection issues to generate errors early.
        // The settings do help but errors may still take many minutes to appear.
        // configuration.set("fs.s3a.connection.timeout", "1000");
        // configuration.set("fs.s3a.connection.establish.timeout", "1");
        configuration.set("fs.s3a.attempts.maximum", "1");
        configuration.set("fs.s3a.retry.limit", "0");
    }

}
