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
package sleeper.utils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import org.apache.hadoop.conf.Configuration;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;

import static sleeper.configuration.properties.instance.CommonProperty.MAXIMUM_CONNECTIONS_TO_S3;
import static sleeper.configuration.properties.instance.QueryProperty.MAXIMUM_CONNECTIONS_TO_S3_FOR_QUERIES;
import static sleeper.configuration.properties.table.TableProperty.S3A_READAHEAD_RANGE;

public class HadoopConfigurationProvider {

    private HadoopConfigurationProvider() {
    }

    public static Configuration getConfigurationForLambdas(InstanceProperties instanceProperties) {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.connection.maximum", instanceProperties.get(MAXIMUM_CONNECTIONS_TO_S3));
        return conf;
    }

    public static Configuration getConfigurationForQueryLambdas(InstanceProperties instanceProperties, TableProperties tableProperties) {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.connection.maximum", instanceProperties.get(MAXIMUM_CONNECTIONS_TO_S3_FOR_QUERIES));
        conf.set("fs.s3a.readahead.range", tableProperties.get(S3A_READAHEAD_RANGE));
        if (System.getenv("AWS_ENDPOINT_URL") != null) {
            setLocalStackConfiguration(conf);
        }
        return conf;
    }

    public static Configuration getConfigurationForECS(InstanceProperties instanceProperties) {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.connection.maximum", instanceProperties.get(MAXIMUM_CONNECTIONS_TO_S3));
        if (System.getenv("AWS_ENDPOINT_URL") != null) {
            setLocalStackConfiguration(conf);
        } else {
            conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper");
        }
        // See https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/performance.html#Improving_data_input_performance_through_fadvise
        // Some quick experiments showed that the following setting increases the number of records processed per second
        // by 21% in comparison to the default value of "normal".
        conf.set("fs.s3a.experimental.input.fadvise", "sequential");
        return conf;
    }

    private static void setLocalStackConfiguration(Configuration conf) {
        conf.set("fs.s3a.endpoint", System.getenv("AWS_ENDPOINT_URL"));
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.aws.credentials.provider", LocalStackCredentialsProvider.class.getName());
        conf.set("fs.s3a.access.key", "test-access-key");
        conf.set("fs.s3a.secret.key", "test-secret-key");
    }

    public static class LocalStackCredentialsProvider implements AWSCredentialsProvider {
        @Override
        public AWSCredentials getCredentials() {
            return new BasicAWSCredentials("test-access-key", "test-secret-key");
        }

        @Override
        public void refresh() {
        }
    }
}
