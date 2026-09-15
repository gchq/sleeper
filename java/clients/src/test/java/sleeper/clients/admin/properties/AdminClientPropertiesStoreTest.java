/*
 * Copyright 2026 Crown Copyright
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
package sleeper.clients.admin.properties;

import org.junit.jupiter.api.Test;

import sleeper.clients.deploy.container.DockerImageConfiguration;
import sleeper.clients.deploy.container.UploadDockerImagesToEcr;
import sleeper.clients.util.cdk.InvokeCdk;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static sleeper.core.properties.table.TableProperty.SCHEMA;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

class AdminClientPropertiesStoreTest {

    @Test
    void shouldExcludeInvalidTableWhenSavingInstanceProperties() throws Exception {
        // Given
        InstanceProperties instanceProperties = createTestInstanceProperties();
        TableProperties validTable = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
        validTable.set(TABLE_NAME, "valid-table");
        TableProperties invalidTable = new TableProperties(instanceProperties);
        invalidTable.set(TABLE_NAME, "invalid-table");
        invalidTable.set(SCHEMA, "{}");

        AdminClientPropertiesStore.Client client = mock(AdminClientPropertiesStore.Client.class);
        TablePropertiesStore tableStore = mock(TablePropertiesStore.class);
        when(client.createTablePropertiesStore(instanceProperties)).thenReturn(tableStore);
        when(tableStore.streamAllTables()).thenReturn(Stream.of(validTable, invalidTable));
        AtomicReference<List<TableProperties>> localTables = new AtomicReference<>();
        doAnswer(invocation -> {
            Stream<TableProperties> tables = invocation.getArgument(1);
            localTables.set(tables.toList());
            return null;
        }).when(client).saveLocalProperties(eq(instanceProperties), any());

        AdminClientPropertiesStore store = new AdminClientPropertiesStore(
                client, mock(InvokeCdk.class), Path.of("generated"),
                mock(UploadDockerImagesToEcr.class), DockerImageConfiguration.getDefault());

        // When
        store.saveInstanceProperties(instanceProperties);

        // Then
        assertThat(localTables.get())
                .extracting(table -> table.get(TABLE_NAME))
                .containsExactly("valid-table");
    }
}
