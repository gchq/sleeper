/*
 * Copyright 2022 Crown Copyright
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

import com.amazonaws.services.s3.AmazonS3;
import com.google.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Abstract class which backs both {@link InstanceProperties} and
 * {@link sleeper.configuration.properties.table.TableProperties}.
 */
public abstract class SleeperProperties<T extends SleeperProperty> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SleeperProperties.class);
    private final Properties properties;

    public SleeperProperties() {
        this(new Properties());
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public SleeperProperties(Properties properties) {
        this.properties = properties;
    }

    protected void init() {
        validate();
    }

    protected abstract void validate();

    public String get(T property) {
        return properties.getProperty(property.getPropertyName(), property.getDefaultValue());
    }

    public Boolean getBoolean(T property) {
        return Boolean.parseBoolean(get(property));
    }

    public Integer getInt(T property) {
        return Integer.parseInt(get(property));
    }

    public Long getLong(T property) {
        return Long.parseLong(get(property));
    }

    public List<String> getList(T property) {
        String value = get(property);
        return value == null ? null : Lists.newArrayList(value.split(","));
    }

    public void setNumber(T property, Number number) {
        if (number != null) {
            set(property, number.toString());
        }
    }

    public void set(T property, String value) {
        if (value != null) {
            properties.setProperty(property.getPropertyName(), value);
        }
    }

    protected Properties getProperties() {
        return properties;
    }

    public Iterator<Map.Entry<Object, Object>> getPropertyIterator() {
        return properties.entrySet().iterator();
    }

    public void load(InputStream inputStream) {
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load store properties file : " + e.getMessage(), e);
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
                throw new RuntimeException("Failed to close store properties stream: " + e.getMessage(), e);
            }
        }
        this.init();
    }

    public void load(File file) throws FileNotFoundException {
        InputStream inputStream = new BufferedInputStream(new FileInputStream(file));
        load(inputStream);
    }

    public void save(OutputStream oututStream) throws IOException {
        properties.store(oututStream, "");
    }

    public void save(File file) throws IOException {
        OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file));
        save(outputStream);
    }

    public String saveAsString() throws IOException {
        StringWriter stringWriter = new StringWriter();
        properties.store(stringWriter, "");
        return stringWriter.toString();
    }

    public void loadFromString(String propertiesAsString) throws IOException {
        StringReader stringReader = new StringReader(propertiesAsString);
        properties.load(stringReader);
        this.init();
    }

    protected void saveToS3(AmazonS3 s3Client, String bucket, String key) throws IOException {
        LOGGER.debug("Uploading config to bucket {}", bucket);
        s3Client.putObject(bucket, key, saveAsString());
    }

    protected void loadFromS3(AmazonS3 s3Client, String bucket, String key) throws IOException {
        String propertiesString = s3Client.getObjectAsString(bucket, key);
        loadFromString(propertiesString);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SleeperProperties that = (SleeperProperties) o;

        return new EqualsBuilder().append(properties, that.properties).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(properties).toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("properties", properties)
                .build();
    }
}
