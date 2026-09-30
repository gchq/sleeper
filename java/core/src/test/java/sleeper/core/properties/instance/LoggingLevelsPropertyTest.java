/*
 * Copyright 2026 Crown Copyright
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
package sleeper.core.properties.instance;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.LoggingLevelsProperty.APACHE_LOGGING_LEVEL;
import static sleeper.core.properties.instance.LoggingLevelsProperty.AWS_LOGGING_LEVEL;
import static sleeper.core.properties.instance.LoggingLevelsProperty.LOGGING_LEVEL;
import static sleeper.core.properties.instance.LoggingLevelsProperty.PARQUET_LOGGING_LEVEL;
import static sleeper.core.properties.instance.LoggingLevelsProperty.ROOT_LOGGING_LEVEL;

class LoggingLevelsPropertyTest {

    @Test
    void shouldMatchLoggingPropertyDefaultsToMainLog4jConfiguration() throws IOException, URISyntaxException {
        Properties log4jProperties = loadMainLog4jProperties();

        assertThat(List.of(
                LOGGING_LEVEL,
                ROOT_LOGGING_LEVEL,
                APACHE_LOGGING_LEVEL,
                PARQUET_LOGGING_LEVEL,
                AWS_LOGGING_LEVEL))
                .allSatisfy(property -> assertThat(property.getDefaultValue())
                        .as(property.getPropertyName())
                        .isEqualTo(log4jProperties.getProperty(property.getPropertyName())));
    }

    private static Properties loadMainLog4jProperties() throws IOException, URISyntaxException {
        Path mainClasses = Path.of(LoggingLevelsProperty.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(mainClasses.resolve("log4j.properties"))) {
            properties.load(input);
        }
        return properties;
    }
}
