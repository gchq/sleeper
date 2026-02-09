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
package sleeper.core.properties.instance;

import sleeper.core.properties.PropertyGroup;
import sleeper.core.properties.SleeperProperties;
import sleeper.core.properties.SleeperPropertiesPrettyPrinter;
import sleeper.core.properties.SleeperPropertyIndex;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static sleeper.core.properties.PropertiesUtils.loadProperties;
import static sleeper.core.properties.instance.CommonProperty.TAGS;

/**
 * Contains values of the properties to configure an instance of Sleeper.
 */
public class InstanceProperties extends SleeperProperties<InstanceProperty> {

    protected Map<String, String> tags = new HashMap<>();

    public InstanceProperties() {
        super();
    }

    protected InstanceProperties(Properties properties) {
        super(properties);
        tags = csvTagsToMap(get(TAGS));
    }

    /**
     * Creates a copy of the given instance properties.
     *
     * @param  instanceProperties the instance properties
     * @return                    the copy
     */
    public static InstanceProperties copyOf(InstanceProperties instanceProperties) {
        return new InstanceProperties(loadProperties(instanceProperties.saveAsString()));
    }

    /**
     * Creates and validates an instance of this class with the given property values.
     *
     * @param  properties the property values
     * @return            the instance properties
     */
    public static InstanceProperties createAndValidate(Properties properties) {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.resetAndValidate(properties);
        return instanceProperties;
    }

    /**
     * Creates an instance of this class with the given property values, without validating the values. This should
     * usually only be used if we need to operate on invalid or incomplete values. It can also be used in other cases
     * where we want to skip validation.
     *
     * @param  properties the property values
     * @return            the instance properties
     */
    public static InstanceProperties createWithoutValidation(Properties properties) {
        return new InstanceProperties(properties);
    }

    @Override
    protected void init() {
        tags = csvTagsToMap(get(TAGS));
        super.init();
    }

    public Map<String, String> getTags() {
        return tags;
    }

    /**
     * Sets the tags instance property from a map of tag name to value.
     *
     * @param tagsMap the map
     */
    public void setTags(Map<String, String> tagsMap) {
        tags.clear();
        tags.putAll(tagsMap);
        set(TAGS, tagsToString(tags));
    }

    /**
     * Sets the tags instance property from a properties file.
     *
     * @param tagsProperties the properties file contents
     */
    public void loadTags(Properties tagsProperties) {
        tags.clear();
        tagsProperties.stringPropertyNames().forEach(tagName -> tags.put(tagName, tagsProperties.getProperty(tagName)));
        if (!tags.isEmpty()) {
            set(TAGS, tagsToString(tags));
        }
    }

    @Override
    public String get(InstanceProperty property) {
        return compute(property, value -> property.computeValue(value, this));
    }

    /**
     * Creates a properties file from the tags instance property.
     *
     * @return the properties file contents
     */
    public Properties getTagsProperties() {
        Properties tagsProperties = new Properties();
        tags.forEach(tagsProperties::setProperty);
        return tagsProperties;
    }

    /**
     * Creates a properties file string from the tags instance property.
     *
     * @return the properties file as a string
     */
    public String getTagsPropertiesAsString() throws IOException {
        StringWriter stringWriter = new StringWriter();
        Properties tagsProperties = getTagsProperties();
        tagsProperties.store(stringWriter, "");
        return stringWriter.toString();
    }

    @Override
    public SleeperPropertyIndex<InstanceProperty> getPropertiesIndex() {
        return InstanceProperty.Index.INSTANCE;
    }

    @Override
    protected SleeperPropertiesPrettyPrinter<InstanceProperty> getPrettyPrinter(PrintWriter writer) {
        return createPrettyPrinter(writer);
    }

    /**
     * Infers the name of the config bucket for a given Sleeper instance.
     *
     * @param  instanceId the Sleeper instance ID
     * @return            the config bucket name
     */
    public static String getConfigBucketFromInstanceId(String instanceId) {
        return String.join("-", "sleeper", instanceId, "config").toLowerCase(Locale.ROOT);
    }

    /**
     * Creates a printer to be used to display instance properties in a given group.
     *
     * @param  writer the writer to write to
     * @param  group  the group to display
     * @return        the pretty printer
     */
    public static SleeperPropertiesPrettyPrinter<InstanceProperty> createPrettyPrinterWithGroup(
            PrintWriter writer, PropertyGroup group) {
        return SleeperPropertiesPrettyPrinter.builder()
                .sortedProperties(InstanceProperty.getAll().stream()
                        .filter(property -> property.getPropertyGroup().equals(group))
                        .collect(Collectors.toList()))
                .writer(writer).hideUnknownProperties(true).build();
    }

    /**
     * Creates a printer to be used to display all instance properties.
     *
     * @param  writer the writer to write to
     * @return        the pretty printer
     */
    public static SleeperPropertiesPrettyPrinter<InstanceProperty> createPrettyPrinter(PrintWriter writer) {
        return SleeperPropertiesPrettyPrinter.builder()
                .properties(InstanceProperty.getAll(), InstancePropertyGroup.getAll())
                .writer(writer).build();
    }

    /**
     * Converts CSV values for the tags instance property into a map of tag name to value. This is the format in which
     * the tags will be held in the instance property.
     *
     * @param  csvTags the instance property value
     * @return         the map of tag name to value
     */
    public static Map<String, String> csvTagsToMap(String csvTags) {
        Map<String, String> tags = new HashMap<>();
        if (null != csvTags && !csvTags.isEmpty()) {
            String[] split = csvTags.split(",");
            if (split.length % 2 == 0) { //Ensure matching number of keys and values
                for (int i = 0; i < split.length; i += 2) {
                    tags.put(split[i], split[i + 1]);
                }
            }
        }
        return tags;
    }

    /**
     * Converts a map of tag name to value into the value for the tags instance property. This is in CSV format.
     *
     * @param  tags the map of tag name to value
     * @return      the instance property value
     */
    public static String tagsToString(Map<String, String> tags) {
        StringBuilder builder = new StringBuilder();
        int count = 0;
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            if (count > 0) {
                builder.append(",");
            }
            builder.append(entry.getKey());
            builder.append(",");
            builder.append(entry.getValue());
            count++;
        }
        return builder.toString();
    }
}
