/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.core.util;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.LoggingLevelsProperty.APACHE_LOGGING_LEVEL;
import static sleeper.core.properties.instance.LoggingLevelsProperty.AWS_LOGGING_LEVEL;
import static sleeper.core.properties.instance.LoggingLevelsProperty.LOGGING_LEVEL;
import static sleeper.core.properties.instance.LoggingLevelsProperty.PARQUET_LOGGING_LEVEL;
import static sleeper.core.properties.instance.LoggingLevelsProperty.ROOT_LOGGING_LEVEL;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

public class EnvironmentUtilsTest {

    private static final String DEFAULT_TOOL_OPTIONS = "" +
            "--add-opens=java.base/java.nio=ALL-UNNAMED " +
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED " +
            "--add-opens=java.base/java.util=ALL-UNNAMED " +
            "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED";

    InstanceProperties instanceProperties = createTestInstanceProperties();

    @Test
    void shouldCreateDefaultEnvironmentWithNoPropertiesSet() {
        // When
        Map<String, String> environment = EnvironmentUtils.createDefaultEnvironmentNoConfigBucket(instanceProperties);

        // Then
        assertThat(environment).isEqualTo(Map.of("JAVA_TOOL_OPTIONS", DEFAULT_TOOL_OPTIONS));
    }

    @Test
    void shouldSetJavaLogLevels() {
        // Given
        instanceProperties.set(LOGGING_LEVEL, "INFO");
        instanceProperties.set(ROOT_LOGGING_LEVEL, "WARN");
        instanceProperties.set(APACHE_LOGGING_LEVEL, "TRACE");
        instanceProperties.set(PARQUET_LOGGING_LEVEL, "DEBUG");
        instanceProperties.set(AWS_LOGGING_LEVEL, "ERROR");

        // When
        Map<String, String> environment = EnvironmentUtils.createDefaultEnvironmentNoConfigBucket(instanceProperties);

        // Then
        assertThat(environment).isEqualTo(Map.of("JAVA_TOOL_OPTIONS", "" +
                "-Dsleeper.logging.level=INFO " +
                "-Dsleeper.logging.root.level=WARN " +
                "-Dsleeper.logging.apache.level=TRACE " +
                "-Dsleeper.logging.parquet.level=DEBUG " +
                "-Dsleeper.logging.aws.level=ERROR " + DEFAULT_TOOL_OPTIONS));
    }

}
