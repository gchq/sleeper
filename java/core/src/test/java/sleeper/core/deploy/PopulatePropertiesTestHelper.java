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
package sleeper.core.deploy;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;

import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

/**
 * Test helpers to generate properties using PopulateInstanceProperties.
 */
public class PopulatePropertiesTestHelper {
    private PopulatePropertiesTestHelper() {
    }

    /**
     * Generates test instance properties using PopulateInstanceProperties.
     *
     * @return the generated properties
     */
    public static InstanceProperties generateTestInstanceProperties() {
        return createTestPopulateInstanceProperties().populate(new InstanceProperties());
    }

    /**
     * Creates a test instance of PopulateInstanceProperties with dummy values.
     *
     * @return the object
     */
    public static PopulateInstanceProperties createTestPopulateInstanceProperties() {
        return testPopulateInstancePropertiesBuilder().build();
    }

    /**
     * Creates a test builder for PopulateInstanceProperties with dummy values.
     *
     * @return the builder
     */
    public static PopulateInstanceProperties.Builder testPopulateInstancePropertiesBuilder() {
        return PopulateInstanceProperties.builder()
                .accountSupplier(() -> "test-account-id").regionIdSupplier(() -> "test-region")
                .instanceId("test-instance").vpcId("some-vpc").subnetIds("some-subnet");
    }

    /**
     * Generates test table properties using PopulateInstanceProperties.
     *
     * @return the generated properties
     */
    public static TableProperties generateTestTableProperties() {
        TableProperties tableProperties = new TableProperties(generateTestInstanceProperties());
        tableProperties.set(TABLE_NAME, "test-table");
        tableProperties.setSchema(schemaWithKey("key"));
        return tableProperties;
    }
}
