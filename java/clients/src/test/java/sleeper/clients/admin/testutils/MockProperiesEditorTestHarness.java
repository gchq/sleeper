/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.clients.admin.testutils;

import sleeper.clients.admin.properties.PropertiesEditor;
import sleeper.clients.admin.properties.UpdatePropertiesRequest;
import sleeper.core.properties.PropertyGroup;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;

import java.io.IOException;

import static org.mockito.Mockito.when;

public class MockProperiesEditorTestHarness implements PropertiesEditorTestHarness {

    private final PropertiesEditor mock;

    public MockProperiesEditorTestHarness(PropertiesEditor mock) {
        this.mock = mock;
    }

    @Override
    public void expectNextEdit(UpdatePropertiesRequest<?> request) {
        try {
            if (request.getPropertiesBefore() instanceof InstanceProperties before) {
                when(mock.openPropertiesFile(before))
                        .thenReturn((UpdatePropertiesRequest<InstanceProperties>) request);
            } else if (request.getPropertiesBefore() instanceof TableProperties before) {
                when(mock.openPropertiesFile(before))
                        .thenReturn((UpdatePropertiesRequest<TableProperties>) request);
            } else {
                throw new IllegalStateException("Unexpected properties type");
            }
        } catch (IOException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void expectNextEdit(UpdatePropertiesRequest<?> request, PropertyGroup group) {
        try {
            if (request.getPropertiesBefore() instanceof InstanceProperties before) {
                when(mock.openPropertiesFile(before, group))
                        .thenReturn((UpdatePropertiesRequest<InstanceProperties>) request);
            } else if (request.getPropertiesBefore() instanceof TableProperties before) {
                when(mock.openPropertiesFile(before, group))
                        .thenReturn((UpdatePropertiesRequest<TableProperties>) request);
            } else {
                throw new IllegalStateException("Unexpected properties type");
            }
        } catch (IOException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

}
