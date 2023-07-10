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

package sleeper.cdk;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;

import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.CommonProperties.ACCOUNT;
import static sleeper.configuration.properties.CommonProperties.ID;
import static sleeper.configuration.properties.CommonProperties.JARS_BUCKET;
import static sleeper.configuration.properties.CommonProperties.REGION;
import static sleeper.configuration.properties.CommonProperties.SUBNETS;
import static sleeper.configuration.properties.CommonProperties.VPC_ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class UtilsTestHelper {
    private UtilsTestHelper() {
    }

    public static Function<String, String> cdkContextWithPropertiesFile(Path tempDir) {
        return Map.of("propertiesfile", tempDir.resolve("instance.properties").toString())::get;
    }

    public static Function<String, String> cdkContextWithPropertiesFileAndSkipVersionCheck(Path tempDir) {
        return Map.of("propertiesfile", tempDir.resolve("instance.properties").toString(),
                "skipVersionCheck", "true")::get;
    }

    public static InstanceProperties createInstancePropertiesWithVersion(String version) {
        InstanceProperties instanceProperties = createUserDefinedInstanceProperties();
        instanceProperties.set(VERSION, version);
        return instanceProperties;
    }

    public static InstanceProperties createUserDefinedInstanceProperties() {
        String id = UUID.randomUUID().toString();
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, id);
        instanceProperties.set(JARS_BUCKET, "");
        instanceProperties.set(ACCOUNT, "");
        instanceProperties.set(REGION, "");
        instanceProperties.set(VPC_ID, "");
        instanceProperties.set(SUBNETS, "");
        return instanceProperties;
    }

    public static TableProperties createUserDefinedTableProperties(InstanceProperties instanceProperties) {
        String id = UUID.randomUUID().toString();
        TableProperties properties = new TableProperties(instanceProperties);
        properties.set(TABLE_NAME, id);
        properties.setSchema(schemaWithKey("key"));
        return properties;
    }
}
