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
package sleeper.core.properties;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;

/**
 * Holds values for Sleeper configuration properties. Abstract class which backs both
 * {@link sleeper.configuration.properties.instance.InstanceProperties} and
 * {@link sleeper.configuration.properties.table.TableProperties}.
 *
 * @param <T> the type of properties held, to ensure only relevant properties are added or retrieved
 */
public abstract class SleeperProperties<T extends SleeperProperty> implements SleeperPropertyValues<T> {
    private final Properties properties;

    protected SleeperProperties() {
        this(new Properties());
    }

    protected SleeperProperties(Properties properties) {
        this.properties = properties;
    }

    /**
     * A method to be called any time the properties are reloaded as a whole. This should perform any operations that
     * would be done in the constructor, and also revalidate the values. This should be overridden if any metadata needs
     * to be set at load time.
     *
     * @throws SleeperPropertiesInvalidException if any value is invalid
     */
    protected void init() throws SleeperPropertiesInvalidException {
        validate();
    }

    /**
     * Validates the values of all properties.
     *
     * @throws SleeperPropertiesInvalidException if any value is invalid
     */
    public final void validate() throws SleeperPropertiesInvalidException {
        SleeperPropertiesValidationReporter reporter = new SleeperPropertiesValidationReporter();
        validate(reporter);
        reporter.throwIfFailed();
    }

    /**
     * Validates the values of all properties, and reports any failues to the given reporter.
     *
     * @param reporter the reporter to receive failures
     */
    public void validate(SleeperPropertiesValidationReporter reporter) {
        getPropertiesIndex().getUserDefined().forEach(property -> {
            String value = get(property);
            if (!property.getValidationPredicate().test(value)) {
                reporter.invalidProperty(property, value);
            }
        });
    }

    /**
     * Retrieves an index of definitions of all properties that can be set for this object. Deals with metadata rather
     * than property values.
     *
     * @return the index
     */
    public abstract SleeperPropertyIndex<T> getPropertiesIndex();

    /**
     * Retrieves a printer to output the property values in a human-readable string format.
     *
     * @param  writer a writer to print to
     * @return        the pretty printer
     */
    protected abstract SleeperPropertiesPrettyPrinter<T> getPrettyPrinter(PrintWriter writer);

    /**
     * Writes the property values to the given writer in a human-readable string format.
     *
     * @param writer the writer
     */
    public void saveUsingPrettyPrinter(PrintWriter writer) {
        this.getPrettyPrinter(writer).print(this);
    }

    @Override
    public abstract String get(T property);

    /**
     * Computes the value of a property. This should be used in implementations of {@link #get}. Handles ignoring empty
     * strings for properties where this is equivalent to no value being set. The post-processing compute method can be
     * used to provide the default value of a property, or handle other logic to replace the value under some condition.
     *
     * @param  property the property
     * @param  compute  the method to post-process the value of a property, including null if it is unset
     * @return          the value of the property
     */
    protected String compute(T property, UnaryOperator<String> compute) {
        String value = properties.getProperty(property.getPropertyName());
        if (property.isIgnoreEmptyValue() && "".equals(value)) {
            value = null;
        }
        return compute.apply(value);
    }

    /**
     * Sets the value of a property. Please call the setter relevant to the type of the property, see other methods on
     * this class.
     *
     * @param property the property
     * @param value    the value, if the property is a string
     */
    public void set(T property, String value) {
        if (value != null) {
            properties.setProperty(property.getPropertyName(), value);
        }
    }

    /**
     * Sets the value of a property. Please call the setter relevant to the type of the property, see other methods on
     * this class.
     *
     * @param property the property
     * @param number   the value, if the property is a number
     */
    public void setNumber(T property, Number number) {
        set(property, number == null ? null : number.toString());
    }

    /**
     * Sets the value of a property. Please call the setter relevant to the type of the property, see other methods on
     * this class.
     *
     * @param property the property
     * @param list     the value, if the property is a list of strings
     */
    public void setList(T property, List<String> list) {
        set(property, String.join(",", list));
    }

    /**
     * Adds any missing values to a property which contains a list of strings. Appends any missing values to the end of
     * the list. Avoids any duplicates.
     *
     * @param property the property
     * @param list     the values to append
     */
    public void addToListIfMissing(T property, List<String> list) {
        List<String> before = getList(property);
        Set<String> beforeSet = new HashSet<>(before);
        List<String> after = Stream.concat(
                before.stream(),
                list.stream().filter(not(beforeSet::contains)))
                .collect(Collectors.toUnmodifiableList());
        setList(property, after);
    }

    /**
     * Sets the value of a property. Please call the setter relevant to the type of the property, see other methods on
     * this class.
     *
     * @param property the property
     * @param value    the value, if the property is an enum type
     */
    public <E extends Enum<E>> void setEnum(T property, E value) {
        set(property, value == null ? null : value.name().toLowerCase(Locale.ROOT));
    }

    /**
     * Sets the value of a property. Please call the setter relevant to the type of the property, see other methods on
     * this class.
     *
     * @param property the property
     * @param list     the value, if the property is a list of an enum type
     */
    public <E extends Enum<E>> void setEnumList(T property, List<E> list) {
        set(property, list == null ? null
                : list.stream()
                        .map(value -> value.toString().toLowerCase(Locale.ROOT))
                        .collect(Collectors.joining(",")));
    }

    /**
     * Removes any value of a property.
     *
     * @param property the property
     */
    public void unset(T property) {
        properties.remove(property.getPropertyName());
    }

    /**
     * Checks if any property has been set with a name that starts with the given prefix.
     *
     * @param  propertyNameStart the prefix to check
     * @return                   true if any property has been set with the prefix
     */
    public boolean isAnyPropertySetStartingWith(String propertyNameStart) {
        return properties.stringPropertyNames().stream().anyMatch(name -> name.startsWith(propertyNameStart));
    }

    /**
     * Checks if a property has been set.
     *
     * @param  property the property
     * @return          true if the property is set to any value
     */
    public boolean isSet(T property) {
        return properties.containsKey(property.getPropertyName()) &&
                !"".equals(properties.getProperty(property.getPropertyName()));
    }

    public Properties getProperties() {
        return properties;
    }

    /**
     * Saves a properties file with the set values to the given output stream. This will only contain the values, and
     * will not contain descriptions or other aids for human readability.
     *
     * @param outputStream the output stream
     */
    public void save(OutputStream outputStream) {
        try {
            properties.store(outputStream, "");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Saves a properties file with the set values. This will only contain the values, and will not contain descriptions
     * or other aids for human readability.
     *
     * @param file the file to save to
     */
    public void save(File file) {
        try {
            OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file));
            save(outputStream);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Saves a properties file with the set values. This will only contain the values, and will not contain descriptions
     * or other aids for human readability.
     *
     * @param file the path to save to
     */
    public void save(Path file) {
        try {
            OutputStream outputStream = new BufferedOutputStream(Files.newOutputStream(file));
            save(outputStream);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Creates a properties file with the set values. This will only contain the values, and will not contain
     * descriptions or other aids for human readability.
     *
     * @return a string with the contents of a properties file
     */
    public String saveAsString() {
        StringWriter stringWriter = new StringWriter();
        try {
            properties.store(stringWriter, "");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return stringWriter.toString();
    }

    /**
     * Clears all values, resets them from the given object, and validates the new values.
     *
     * @param newProperties the properties to set
     */
    public void resetAndValidate(Properties newProperties) {
        properties.clear();
        properties.putAll(newProperties);
        init();
    }

    /**
     * Retrieves the names of any properties that have values but are not present in the property metadata index.
     *
     * @return the entries for the values of unknown properties
     */
    public Stream<Map.Entry<String, String>> getUnknownProperties() {
        return properties.stringPropertyNames().stream()
                .filter(not(this::isKnownProperty))
                .map(name -> Map.entry(name, properties.getProperty(name)));
    }

    private boolean isKnownProperty(String propertyName) {
        return getPropertiesIndex().getByName(propertyName).isPresent();
    }

    /**
     * Converts and copies all set values of properties into a hash map.
     *
     * @return the copied map
     */
    public Map<String, String> toMap() {
        return PropertiesUtils.toMap(properties);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SleeperProperties<?> that = (SleeperProperties<?>) o;

        return new EqualsBuilder().append(properties, that.properties).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(properties).toHashCode();
    }

    @Override
    public String toString() {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        getPrettyPrinter(new PrintWriter(stream, false, StandardCharsets.UTF_8)).print(this);
        return stream.toString(StandardCharsets.UTF_8);
    }
}
