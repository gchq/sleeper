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
import sleeper.core.properties.SleeperProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;

public class InMemoryPropertiesEditor implements PropertiesEditor, PropertiesEditorTestHarness {

    private final Queue<ExpectedEdit> edits = new LinkedList<>();

    @Override
    public void expectNextEdit(UpdatePropertiesRequest<?> request) {
        edits.add(new ExpectedEdit(request, null));
    }

    @Override
    public void expectNextEdit(UpdatePropertiesRequest<?> request, PropertyGroup group) {
        edits.add(new ExpectedEdit(request, group));
    }

    @Override
    public UpdatePropertiesRequest<InstanceProperties> openPropertiesFile(InstanceProperties properties) throws IOException, InterruptedException {
        return getNextRequest(properties, null);
    }

    @Override
    public UpdatePropertiesRequest<TableProperties> openPropertiesFile(TableProperties properties) throws IOException, InterruptedException {
        return getNextRequest(properties, null);
    }

    @Override
    public UpdatePropertiesRequest<InstanceProperties> openPropertiesFile(InstanceProperties properties, PropertyGroup propertyGroup) throws IOException, InterruptedException {
        return getNextRequest(properties, propertyGroup);
    }

    @Override
    public UpdatePropertiesRequest<TableProperties> openPropertiesFile(TableProperties properties, PropertyGroup propertyGroup) throws IOException, InterruptedException {
        return getNextRequest(properties, propertyGroup);
    }

    private <T extends SleeperProperties<?>> UpdatePropertiesRequest<T> getNextRequest(T properties, PropertyGroup propertyGroup) {
        ExpectedEdit edit = edits.poll();
        if (edit == null || !edit.isBefore(properties) || propertyGroup != edit.group()) {
            throw new IllegalStateException("Unexpected request to open properties editor for " + properties.getClass().getSimpleName());
        }
        return (UpdatePropertiesRequest<T>) edit.request();
    }

    private record ExpectedEdit(UpdatePropertiesRequest<?> request, PropertyGroup group) {

        public boolean isBefore(SleeperProperties<?> before) {
            return Objects.equals(request.getPropertiesBefore(), before);
        }
    }

}
