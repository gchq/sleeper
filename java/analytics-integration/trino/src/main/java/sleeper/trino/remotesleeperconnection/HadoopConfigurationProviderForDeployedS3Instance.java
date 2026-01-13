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
package sleeper.trino.remotesleeperconnection;

import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.parquet.utils.HadoopConfigurationProvider.getConfigurationForECS;

public class HadoopConfigurationProviderForDeployedS3Instance implements HadoopConfigurationProvider {
    /**
     * Set up the Hadoop configuration to read Parquet files from S3. This will be used by the ParquetReader.
     * <p>
     * Hadoop uses the Thread context classloader by default. This classloader is different from the classloader that
     * Trino provides to individual plugins and so some of the classes that we would expect to be available to Hadoop
     * are not visible by default when it runs inside a Trino plugin. It causes truly horrible bugs which are difficult
     * to track down. Setting the classloader to the same one that is used by this class resolves the issue.
     * <p>
     * The configuration details are taken from {@link sleeper.parquet.utils.HadoopConfigurationProvider}.
     *
     * @return The Hadoop configuration.
     */
    @Override
    public Configuration getHadoopConfiguration(InstanceProperties instanceProperties) {
        Configuration configuration = getConfigurationForECS(instanceProperties);
        configuration.setClassLoader(HadoopConfigurationProviderForDeployedS3Instance.class.getClassLoader());
        configuration.set("fs.s3a.aws.credentials.provider", DefaultCredentialsProvider.class.getName());
        configuration.set("fs.s3a.impl", org.apache.hadoop.fs.s3a.S3AFileSystem.class.getName());
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        return configuration;
    }
}
