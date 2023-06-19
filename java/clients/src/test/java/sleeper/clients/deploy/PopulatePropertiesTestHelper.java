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
package sleeper.clients.deploy;

import software.amazon.awssdk.regions.Region;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;

import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class PopulatePropertiesTestHelper {
    private PopulatePropertiesTestHelper() {
    }

    public static InstanceProperties generateTestInstanceProperties() {
        return PopulateInstanceProperties.builder()
                .accountSupplier(() -> "test-account-id").regionProvider(() -> Region.AWS_GLOBAL)
                .instanceId("test-instance").vpcId("some-vpc").subnetId("some-subnet").build().populate();
    }

    public static TableProperties generateTestTableProperties() {
        return GenerateTableProperties.from(generateTestInstanceProperties(), schemaWithKey("key"), "test-table");
    }
}
