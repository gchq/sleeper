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

package sleeper.table.store.dynamodb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.table.TableId;
import sleeper.core.table.TableIdStore;
import sleeper.dynamodb.tools.DynamoDBTestBase;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;

public class DynamoDBTableIdStoreIT extends DynamoDBTestBase {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableIdStore store = new DynamoDBTableIdStore(dynamoDBClient, instanceProperties);

    @BeforeEach
    void setUp() {
        DynamoDBTableIdStoreCreator.create(dynamoDBClient, instanceProperties);
    }

    @Test
    void shouldCreateATable() {
        TableId tableId = store.createTable("test-table");

        assertThat(store.streamAllTables())
                .containsExactly(tableId);
    }
}
