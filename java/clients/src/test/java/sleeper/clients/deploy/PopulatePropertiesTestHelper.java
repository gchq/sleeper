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
package sleeper.clients.deploy;

import software.amazon.awssdk.regions.Region;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;

import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class PopulatePropertiesTestHelper {
    private PopulatePropertiesTestHelper() {
    }

    public static InstanceProperties generateTestInstanceProperties() {
        return PopulateInstancePropertiesAws.builder()
                .accountSupplier(() -> "test-account-id").regionProvider(() -> Region.AWS_GLOBAL)
                .instanceId("test-instance").vpcId("some-vpc").subnetIds("some-subnet").build().populate();
    }

    public static TableProperties generateTestTableProperties() {
        TableProperties tableProperties = new TableProperties(generateTestInstanceProperties());
        tableProperties.set(TABLE_NAME, "test-table");
        tableProperties.setSchema(schemaWithKey("key"));
        return tableProperties;
    }
}
