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
package sleeper.configuration.properties.instance;

import com.amazonaws.services.s3.AmazonS3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.SleeperProperties;
import sleeper.configuration.properties.SleeperPropertyIndex;
import sleeper.configuration.properties.format.SleeperPropertiesPrettyPrinter;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static sleeper.configuration.properties.PropertiesUtils.loadProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.TAGS;

/**
 * Contains all the properties needed to deploy an instance of Sleeper.
 */
public class InstanceProperties extends SleeperProperties<InstanceProperty> {
    private static final Logger LOGGER = LoggerFactory.getLogger(InstanceProperties.class);

    public static final String S3_INSTANCE_PROPERTIES_FILE = "instance.properties";

    protected Map<String, String> tags = new HashMap<>();

    public InstanceProperties() {
        super();
    }

    protected InstanceProperties(Properties properties) {
        super(properties);
        tags = csvTagsToMap(get(TAGS));
    }

    public static InstanceProperties copyOf(InstanceProperties instanceProperties) {
        return new InstanceProperties(loadProperties(instanceProperties.saveAsString()));
    }

    public static InstanceProperties createAndValidate(Properties properties) {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.resetAndValidate(properties);
        return instanceProperties;
    }

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

    public void setTags(Map<String, String> tagsMap) {
        tags.clear();
        tags.putAll(tagsMap);
        set(TAGS, tagsToString(tags));
    }

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

    public Properties getTagsProperties() {
        Properties tagsProperties = new Properties();
        tags.forEach(tagsProperties::setProperty);
        return tagsProperties;
    }

    public String getTagsPropertiesAsString() throws IOException {
        StringWriter stringWriter = new StringWriter();
        Properties tagsProperties = getTagsProperties();
        tagsProperties.store(stringWriter, "");
        return stringWriter.toString();
    }

    public void loadFromS3GivenInstanceId(AmazonS3 s3Client, String instanceId) {
        String configBucket = S3InstanceProperties.getConfigBucketFromInstanceId(instanceId);
        loadFromS3(s3Client, configBucket);
    }

    private void loadFromS3(AmazonS3 s3Client, String bucket) {
        String propertiesString = s3Client.getObjectAsString(bucket, S3_INSTANCE_PROPERTIES_FILE);
        resetAndValidate(loadProperties(propertiesString));
    }

    public void saveToS3(AmazonS3 s3Client) {
        String bucket = get(CONFIG_BUCKET);
        LOGGER.debug("Uploading config to bucket {}", bucket);
        s3Client.putObject(bucket, S3_INSTANCE_PROPERTIES_FILE, saveAsString());
        LOGGER.info("Saved instance properties to bucket {}, key {}", get(CONFIG_BUCKET), S3_INSTANCE_PROPERTIES_FILE);
    }

    @Override
    public SleeperPropertyIndex<InstanceProperty> getPropertiesIndex() {
        return InstanceProperty.Index.INSTANCE;
    }

    @Override
    protected SleeperPropertiesPrettyPrinter<InstanceProperty> getPrettyPrinter(PrintWriter writer) {
        return SleeperPropertiesPrettyPrinter.forInstanceProperties(writer);
    }

    public static Map<String, String> csvTagsToMap(String csvTags) {
        Map<String, String> tags = new HashMap<>();
        if (null != csvTags && !csvTags.isEmpty()) {
            String[] split = csvTags.split(",");
            for (int i = 0; i < split.length; i += 2) {
                tags.put(split[i], split[i + 1]);
            }
        }
        return tags;
    }

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
