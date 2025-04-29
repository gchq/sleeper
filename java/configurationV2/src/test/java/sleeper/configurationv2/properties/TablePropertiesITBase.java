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
package sleeper.configurationv2.properties;

import org.junit.jupiter.api.BeforeEach;

import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.localstack.test.LocalStackTestBase;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;

/**
 * A base class for tests storing table properties in S3 with a DynamoDB table index, in LocalStack.
 */
public abstract class TablePropertiesITBase extends LocalStackTestBase {

    private static final Schema KEY_VALUE_SCHEMA = Schema.builder()
            .rowKeyFields(new Field("key", new StringType()))
            .valueFields(new Field("value", new StringType()))
            .build();

    protected final InstanceProperties instanceProperties = createTestInstanceProperties();
    protected final TableProperties tableProperties = createValidTableProperties();
    protected final TablePropertiesStore store = S3TableProperties.createStore(instanceProperties, s3ClientV2, dynamoClient);
    protected final String tableName = tableProperties.get(TABLE_NAME);
    protected final String tableId = tableProperties.get(TABLE_ID);

    @BeforeEach
    void setUp() {
        s3ClientV2.createBucket(builder -> builder.bucket(instanceProperties.get(CONFIG_BUCKET)));
        DynamoDBTableIndexCreator.create(dynamoClient, instanceProperties);
    }

    protected TableProperties createValidTableProperties() {
        return createTestTableProperties(instanceProperties, KEY_VALUE_SCHEMA);
    }
}
