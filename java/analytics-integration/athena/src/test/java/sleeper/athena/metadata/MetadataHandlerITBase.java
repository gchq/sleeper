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
package sleeper.athena.metadata;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import sleeper.athena.TestUtils;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.localstack.test.LocalStackTestBase;

import java.io.IOException;
import java.nio.file.Path;

import static java.nio.file.Files.createTempDirectory;

public abstract class MetadataHandlerITBase extends LocalStackTestBase {

    // For storing data
    @TempDir
    public static Path tempDir;

    // System properties used in this class are based on the AWS V2 values. See this page for details:
    // https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/migration-env-and-system-props.html
    private static final String ACCESS_KEY_SYSTEM_PROPERTY = "aws.accessKeyId";
    private static final String AWS_REGION_SYSTEM_PROPERTY = "aws.region";
    private static final String SECRET_KEY_SYSTEM_PROPERTY = "aws.secretAccessKey";

    protected static final Schema TIME_SERIES_SCHEMA = Schema.builder()
            .rowKeyFields(
                    new Field("year", new IntType()),
                    new Field("month", new IntType()),
                    new Field("day", new IntType()))
            .valueFields(new Field("count", new LongType()))
            .build();

    @BeforeEach
    public void setUpCredentials() {

        // Annoyingly the MetadataHandler hard-codes the S3 client it uses to check the spill bucket. Therefore
        // I need to set up some credentials in System properties so the default client will pick them up.
        System.setProperty(ACCESS_KEY_SYSTEM_PROPERTY, localStackContainer.getAccessKey());
        System.setProperty(SECRET_KEY_SYSTEM_PROPERTY, localStackContainer.getSecretKey());
        System.setProperty(AWS_REGION_SYSTEM_PROPERTY, localStackContainer.getRegion());
    }

    @AfterEach
    public void clearUpCredentials() {
        System.clearProperty(ACCESS_KEY_SYSTEM_PROPERTY);
        System.clearProperty(SECRET_KEY_SYSTEM_PROPERTY);
        System.clearProperty(AWS_REGION_SYSTEM_PROPERTY);
    }

    protected InstanceProperties createInstance() throws IOException {
        return TestUtils.createInstance(s3Client, dynamoClient,
                createTempDirectory(tempDir, null).toString());
    }

    protected TableProperties createEmptyTable(InstanceProperties instanceProperties) {
        return TestUtils.createTable(instanceProperties, TIME_SERIES_SCHEMA,
                s3Client, dynamoClient, 2018, 2019, 2020);
    }

    protected TableProperties createTable(InstanceProperties instanceProperties) throws IOException {
        TableProperties table = createEmptyTable(instanceProperties);
        TestUtils.ingestData(s3Client, dynamoClient, createTempDirectory(tempDir, null).toString(),
                instanceProperties, table);
        return table;
    }
}
