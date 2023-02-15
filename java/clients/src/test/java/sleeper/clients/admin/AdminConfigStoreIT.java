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

package sleeper.clients.admin;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.clients.admin.testutils.AdminClientITBase;
import sleeper.configuration.properties.InstanceProperties;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FARGATE_VERSION;
import static sleeper.configuration.properties.table.TableProperty.ROW_GROUP_SIZE;

public class AdminConfigStoreIT extends AdminClientITBase {
    @DisplayName("Update instance properties")
    @Nested
    class UpdateInstanceProperties {
        @BeforeEach
        void setUp() throws IOException {
            createValidInstanceProperties().saveToS3(s3);
        }

        @Test
        void shouldUpdateInstanceProperty() {
            // When
            store().updateInstanceProperty(INSTANCE_ID, FARGATE_VERSION, "1.2.3");

            // Then
            assertThat(store().loadInstanceProperties(INSTANCE_ID).get(FARGATE_VERSION))
                    .isEqualTo("1.2.3");
        }
    }

    @DisplayName("Update table properties")
    @Nested
    class UpdateTableProperties {
        @BeforeEach
        void setUp() throws IOException {
            InstanceProperties instanceProperties = createValidInstanceProperties();
            instanceProperties.saveToS3(s3);
            createValidTableProperties(instanceProperties).saveToS3(s3);
        }

        @Test
        void shouldUpdateTableProperty() {
            // When
            store().updateTableProperty(INSTANCE_ID, TABLE_NAME_VALUE, ROW_GROUP_SIZE, "123");

            // Then
            assertThat(store().loadTableProperties(INSTANCE_ID, TABLE_NAME_VALUE).getInt(ROW_GROUP_SIZE))
                    .isEqualTo(123);
        }
    }
}
