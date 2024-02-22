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

package sleeper.configuration.table.index;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.table.TableNotFoundException;
import sleeper.core.table.TableStatus;
import sleeper.dynamodb.tools.DynamoDBTestBase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;

public class DynamoDBTableIndexDynamoSpecificIT extends DynamoDBTestBase {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final DynamoDBTableIndex index = new DynamoDBTableIndex(instanceProperties, dynamoDBClient);

    @BeforeEach
    void setUp() {
        DynamoDBTableIndexCreator.create(dynamoDBClient, instanceProperties);
    }

    @Test
    void shouldFailToUpdateTableIfTableDeletedAfterLoadingOldId() {
        // Given
        TableStatus oldTable = TableStatus.uniqueIdAndName("test-id", "old-name");
        TableStatus newTable = TableStatus.uniqueIdAndName("test-id", "new-name");

        // When/Then
        assertThatThrownBy(() -> index.update(oldTable, newTable))
                .isInstanceOf(TableNotFoundException.class);
        assertThat(index.streamAllTables()).isEmpty();
    }

    @Test
    void shouldFailToUpdateTableIfTableRenamedAfterLoadingOldId() {
        // Given
        TableStatus oldTable = TableStatus.uniqueIdAndName("test-id", "old-name");
        TableStatus renamedTable = TableStatus.uniqueIdAndName("test-id", "changed-name");
        TableStatus newTable = TableStatus.uniqueIdAndName("test-id", "new-name");
        index.create(oldTable);
        index.update(renamedTable);

        // When/Then
        assertThatThrownBy(() -> index.update(oldTable, newTable))
                .isInstanceOf(TableNotFoundException.class);
        assertThat(index.streamAllTables()).contains(renamedTable);
    }
}
