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
package sleeper.parquet.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;

import static sleeper.core.properties.instance.CommonProperty.MAXIMUM_CONNECTIONS_TO_S3;
import static sleeper.core.properties.instance.CommonProperty.S3_UPLOAD_BLOCK_SIZE;
import static sleeper.core.properties.instance.QueryProperty.MAXIMUM_CONNECTIONS_TO_S3_FOR_QUERIES;
import static sleeper.core.properties.table.TableProperty.S3A_READAHEAD_RANGE;

public class HadoopConfigurationProvider {

    private HadoopConfigurationProvider() {
    }

    public static Configuration getConfigurationForClient() {
        Configuration conf = new Configuration();
        if (System.getenv("AWS_ENDPOINT_URL") != null) {
            setLocalStackConfiguration(conf);
        } else {
            conf.set("fs.s3a.aws.credentials.provider", DefaultCredentialsProvider.class.getName());
        }
        return conf;
    }

    public static Configuration getConfigurationForClient(InstanceProperties instanceProperties) {
        Configuration conf = getConfigurationForClient();
        conf.set("fs.s3a.connection.maximum", instanceProperties.get(MAXIMUM_CONNECTIONS_TO_S3_FOR_QUERIES));
        return conf;
    }

    public static Configuration getConfigurationForClient(InstanceProperties instanceProperties, TableProperties tableProperties) {
        Configuration conf = getConfigurationForClient(instanceProperties);
        conf.set("fs.s3a.readahead.range", tableProperties.get(S3A_READAHEAD_RANGE));
        return conf;
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
        } else {
            conf.set("fs.s3a.aws.credentials.provider", DefaultCredentialsProvider.class.getName());
        }
        return conf;
    }

    public static Configuration getConfigurationForEKS(InstanceProperties instanceProperties) {
        Configuration configuration = getConfigurationForECS(instanceProperties);
        configuration.set("fs.s3a.aws.credentials.provider", DefaultCredentialsProvider.class.getName());
        return configuration;
    }

    public static Configuration getConfigurationForEMR(InstanceProperties instanceProperties) {
        return getConfigurationForECS(instanceProperties);
    }

    public static Configuration getConfigurationForECS(InstanceProperties instanceProperties) {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.connection.maximum", instanceProperties.get(MAXIMUM_CONNECTIONS_TO_S3));
        conf.set("fs.s3a.block.size", instanceProperties.get(S3_UPLOAD_BLOCK_SIZE));
        conf.set("fs.s3a.bucket.probe", "0");
        conf.set("fs.s3a.fast.upload", "true");
        if (System.getenv("AWS_ENDPOINT_URL") != null) {
            setLocalStackConfiguration(conf);
        } else {
            conf.set("fs.s3a.aws.credentials.provider", ContainerCredentialsProvider.class.getName());
        }
        // See https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/performance.html#Improving_data_input_performance_through_fadvise
        // Some quick experiments showed that the following setting increases the number of rows processed per second
        // by 21% in comparison to the default value of "normal".
        conf.set("fs.s3a.experimental.input.fadvise", "sequential");
        return conf;
    }

    private static void setLocalStackConfiguration(Configuration conf) {
        conf.set("fs.s3a.endpoint", System.getenv("AWS_ENDPOINT_URL"));
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.aws.credentials.provider", SimpleAWSCredentialsProvider.class.getName());
        conf.set("fs.s3a.access.key", "test-access-key");
        conf.set("fs.s3a.secret.key", "test-secret-key");
    }
}
