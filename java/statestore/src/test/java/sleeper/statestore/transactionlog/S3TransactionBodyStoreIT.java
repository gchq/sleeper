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
package sleeper.statestore.transactionlog;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.localstack.test.SleeperLocalStackContainer;
import sleeper.parquet.utils.HadoopConfigurationLocalStackUtils;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.localstack.test.LocalStackAwsV1ClientHelper.buildAwsV1Client;

@Testcontainers
public class S3TransactionBodyStoreIT {

    @Container
    public static LocalStackContainer localStackContainer = SleeperLocalStackContainer.create(LocalStackContainer.Service.S3);
    protected static AmazonS3 s3Client;
    protected final InstanceProperties instanceProperties = createTestInstanceProperties();
    protected final TableProperties tableProperties = null;
    protected final Configuration configuration = HadoopConfigurationLocalStackUtils.getHadoopConfiguration(localStackContainer);

    @BeforeAll
    public static void initClient() {
        s3Client = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    }

    @BeforeEach
    void setUp() {
        //new S3TransactionBodyStore(instanceProperties, tableProperties, s3Client);
        s3Client.createBucket(instanceProperties.get(DATA_BUCKET));
    }

    @Test
    @Disabled
    void shouldProvideTransactionBodyStoreForFilesWithGivenTableId() {

    }

    @Test
    @Disabled
    void shouldProvideTransactionBodyStoreWhenGivenTableProperties() {

    }

}
