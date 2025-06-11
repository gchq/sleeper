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
package sleeper.statestore;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class StateStoreFactoryIT extends LocalStackTestBase {

    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));

    @Test
    void shouldCreateStateStoreBySimpleClassName() {
        // Given
        tableProperties.set(STATESTORE_CLASSNAME, DynamoDBTransactionLogStateStore.class.getSimpleName());

        // When / Then
        assertThat(factory().getStateStore(tableProperties)).isNotNull();
    }

    @Test
    void shouldCreateStateStoreByFullClassName() {
        // Given
        tableProperties.set(STATESTORE_CLASSNAME, DynamoDBTransactionLogStateStore.class.getName());

        // When / Then
        assertThat(factory().getStateStore(tableProperties)).isNotNull();
    }

    @Test
    void shouldCreateStateStoreByNonMatchingFullClassName() {
        // Given
        tableProperties.set(STATESTORE_CLASSNAME, "not.a.match." + DynamoDBTransactionLogStateStore.class.getSimpleName());

        // When / Then
        assertThat(factory().getStateStore(tableProperties)).isNotNull();
    }

    @Test
    void shouldNotCreateStateStoreWhenClassNameDoesNotMatch() {
        // Given
        tableProperties.set(STATESTORE_CLASSNAME, "NotAStateStore");

        // When / Then
        assertThatThrownBy(() -> factory().getStateStore(tableProperties))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Unknown StateStore class: NotAStateStore");
    }

    private StateStoreFactory factory() {
        return new StateStoreFactory(instanceProperties, s3ClientV2, dynamoClientV2);
    }

}
