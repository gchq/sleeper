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
package sleeper.systemtest.configuration;

import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.List;
import java.util.function.Function;

import static sleeper.configuration.properties.instance.CommonProperty.EDIT_TABLES_ROLE;
import static sleeper.configuration.properties.instance.CommonProperty.REPORTING_ROLE;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_SOURCE_ROLE;
import static sleeper.configuration.properties.instance.QueryProperty.QUERY_ROLE;

public class SystemTestRole {

    private SystemTestRole() {
    }

    public static void addSystemTestRole(InstanceProperties properties, Function<String, String> tryGetContext) {
        addSystemTestRole(properties, tryGetContext, System::getenv);
    }

    public static void addSystemTestRole(
            InstanceProperties properties, Function<String, String> tryGetContext, Function<String, String> getenv) {
        String systemTestRole = tryGetContext.apply("role");
        if (systemTestRole == null) {
            systemTestRole = getenv.apply("SLEEPER_SYSTEM_TEST_ROLE");
        }
        addSystemTestRole(properties, systemTestRole);
    }

    public static void addSystemTestRole(InstanceProperties properties, String systemTestRole) {
        if (systemTestRole != null) {
            properties.addToList(INGEST_SOURCE_ROLE, List.of(systemTestRole));
            properties.addToList(QUERY_ROLE, List.of(systemTestRole));
            properties.addToList(EDIT_TABLES_ROLE, List.of(systemTestRole));
            properties.addToList(REPORTING_ROLE, List.of(systemTestRole));
        }
    }

}
