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
package sleeper.clients.admin.properties;

import sleeper.core.properties.PropertyGroup;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;

import java.io.IOException;

/**
 * A port to invoke an editor where the user can view/edit Sleeper configuration properties.
 */
public interface PropertiesEditor {

    /**
     * Opens an editor to view/edit all instance properties.
     *
     * @param  properties           the instance properties
     * @return                      the changes made in the editor
     * @throws IOException          if an I/O problem occurs
     * @throws InterruptedException if the thread is interrupted waiting for the user to finish
     */
    UpdatePropertiesRequest<InstanceProperties> openPropertiesFile(InstanceProperties properties) throws IOException, InterruptedException;

    /**
     * Opens an editor to view/edit all table properties.
     *
     * @param  properties           the table properties
     * @return                      the changes made in the editor
     * @throws IOException          if an I/O problem occurs
     * @throws InterruptedException if the thread is interrupted waiting for the user to finish
     */
    UpdatePropertiesRequest<TableProperties> openPropertiesFile(TableProperties properties) throws IOException, InterruptedException;

    /**
     * Opens an editor to view/edit a single instance property group.
     *
     * @param  properties           the instance properties
     * @param  propertyGroup        the property group to view/edit
     * @return                      the changes made in the editor
     * @throws IOException          if an I/O problem occurs
     * @throws InterruptedException if the thread is interrupted waiting for the user to finish
     */
    UpdatePropertiesRequest<InstanceProperties> openPropertiesFile(
            InstanceProperties properties, PropertyGroup propertyGroup) throws IOException, InterruptedException;

    /**
     * Opens an editor to view/edit a single table property group.
     *
     * @param  properties           the table properties
     * @param  propertyGroup        the property group to view/edit
     * @return                      the changes made in the editor
     * @throws IOException          if an I/O problem occurs
     * @throws InterruptedException if the thread is interrupted waiting for the user to finish
     */
    UpdatePropertiesRequest<TableProperties> openPropertiesFile(
            TableProperties properties, PropertyGroup propertyGroup) throws IOException, InterruptedException;

}
