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
package sleeper.systemtest.configuration;

import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.systemtest.configuration.IngestMode.DIRECT;
import static sleeper.systemtest.configuration.SystemTestProperty.INGEST_MODE;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_RECORDS_PER_WRITER;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_WRITERS;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_REPO;

public class SystemTestPropertiesTestHelper {

    private SystemTestPropertiesTestHelper() {
    }

    public static SystemTestProperties createTestSystemTestProperties() {
        SystemTestProperties properties = new SystemTestProperties();
        properties.set(NUMBER_OF_WRITERS, "1");
        properties.set(NUMBER_OF_RECORDS_PER_WRITER, "5");
        properties.set(INGEST_MODE, DIRECT.name());
        properties.set(SYSTEM_TEST_REPO, "test-repo");
        properties.loadFromString(createTestInstanceProperties().saveAsString());
        return properties;
    }
}
