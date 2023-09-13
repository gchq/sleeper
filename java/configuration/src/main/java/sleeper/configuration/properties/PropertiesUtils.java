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
package sleeper.configuration.properties;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.convert.DisabledListDelimiterHandler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class PropertiesUtils {

    private PropertiesUtils() {
    }

    public static Properties loadProperties(Path file) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(file)) {
            return loadProperties(reader);
        }
    }

    public static Properties loadProperties(String input) throws IOException {
        return loadProperties(new StringReader(input));
    }

    public static Properties loadProperties(Reader reader) throws IOException {
        Properties properties = new Properties();
        properties.load(reader);
        return properties;
    }

    public static PropertiesConfiguration.PropertiesWriter buildPropertiesWriter(Writer writer) {
        PropertiesConfiguration.PropertiesWriter propertiesWriter = new PropertiesConfiguration.PropertiesWriter(
                writer, new DisabledListDelimiterHandler());
        propertiesWriter.setGlobalSeparator("=");
        return propertiesWriter;
    }

    public static Map<String, String> toMap(Properties properties) {
        return properties.stringPropertyNames().stream()
                .collect(Collectors.toUnmodifiableMap(
                        propertyName -> propertyName,
                        properties::getProperty));
    }
}
