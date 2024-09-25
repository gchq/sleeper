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

package sleeper.statestore.s3;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.dynamodb.test.DynamoDBTestBase;

import java.nio.file.Path;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.instance.CommonProperty.MAXIMUM_CONNECTIONS_TO_S3;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

public abstract class S3StateStoreTestBase extends DynamoDBTestBase {
    protected final InstanceProperties instanceProperties = createTestInstanceProperties();

    @TempDir
    public Path tempDir;

    @BeforeEach
    void setUpBase() {
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.setNumber(MAXIMUM_CONNECTIONS_TO_S3, 5);
        instanceProperties.set(DATA_BUCKET, tempDir.toString());
        new S3StateStoreCreator(instanceProperties, dynamoDBClient).create();
    }
}
