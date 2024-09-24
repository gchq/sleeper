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
package sleeper.configuration.properties;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.convert.DisabledListDelimiterHandler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Utilities to read and write properties configuration files.
 */
public class PropertiesUtils {

    private PropertiesUtils() {
    }

    /**
     * Loads a properties file from the file system.
     *
     * @param  file the path to the file
     * @return      the properties file contents
     */
    public static Properties loadProperties(Path file) {
        try (BufferedReader reader = Files.newBufferedReader(file)) {
            return loadProperties(reader);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Loads a properties file from the file system.
     *
     * @param  file the path to the file
     * @return      the properties file contents
     */
    public static Properties loadProperties(File file) {
        try (BufferedReader reader = new BufferedReader(new FileReader(file, StandardCharsets.UTF_8))) {
            return loadProperties(reader);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Loads properties from a string.
     *
     * @param  input the string
     * @return       the properties
     */
    public static Properties loadProperties(String input) {
        return loadProperties(new StringReader(input));
    }

    /**
     * Loads a properties file from a reader.
     *
     * @param  reader the reader
     * @return        the properties file contents
     */
    public static Properties loadProperties(Reader reader) {
        Properties properties = new Properties();
        try {
            properties.load(reader);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return properties;
    }

    /**
     * Creates a properties writer with settings for Sleeper.
     *
     * @param  writer the writer to write to
     * @return        the properties writer
     */
    public static PropertiesConfiguration.PropertiesWriter buildPropertiesWriter(Writer writer) {
        PropertiesConfiguration.PropertiesWriter propertiesWriter = new PropertiesConfiguration.PropertiesWriter(
                writer, new DisabledListDelimiterHandler());
        propertiesWriter.setGlobalSeparator("=");
        return propertiesWriter;
    }

    /**
     * Converts a properties object to a map from property name to value.
     *
     * @param  properties the properties
     * @return            the map
     */
    public static Map<String, String> toMap(Properties properties) {
        return properties.stringPropertyNames().stream()
                .collect(Collectors.toUnmodifiableMap(
                        propertyName -> propertyName,
                        properties::getProperty));
    }
}
