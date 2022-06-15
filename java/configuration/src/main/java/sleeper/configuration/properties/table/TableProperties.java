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
package sleeper.configuration.properties.table;

import com.amazonaws.services.s3.AmazonS3;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.InstanceProperty;
import sleeper.configuration.properties.SleeperProperties;
import sleeper.configuration.properties.SleeperProperty;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;

import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class TableProperties extends SleeperProperties<ITableProperty> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableProperties.class);

    public static final String TABLES_PREFIX = "tables";
    // Schema is cached for faster access
    private Schema schema;
    private final InstanceProperties instanceProperties; // Used for default properties

    public TableProperties(InstanceProperties instanceProperties) {
        this.instanceProperties = instanceProperties;
    }

    @Override
    protected void init() {
        String schemaFile = get(TableProperty.SCHEMA_FILE);
        String schema = get(TableProperty.SCHEMA);
        SchemaSerDe schemaSerDe = new SchemaSerDe();
        if (schema == null) {
            if (schemaFile != null) {
                try {
                    setSchema(schemaSerDe.fromJsonFile(schemaFile));
                } catch (FileNotFoundException e) {
                    throw new RuntimeException("Unable to load file from JSON", e);
                }
            }
        } else {
            this.schema = schemaSerDe.fromJson(schema);
        }
        super.init();
    }

    @Override
    protected void validate() {
        Arrays.stream(TableProperty.values()).filter(prop -> !prop.validationPredicate().test(get(prop)))
                .forEach(prop -> {
                    throw new IllegalArgumentException("Property " + prop.getPropertyName() +
                            " was invalid. It was \"" + get(prop) + "\""); });
    }

    @Override
    public String get(ITableProperty property) {
        SleeperProperty defaultProperty = property.getDefaultProperty();
        if (defaultProperty == null) {
            return super.get(property);
        } else if (defaultProperty instanceof ITableProperty) {
            return getProperties().getProperty(property.getPropertyName(), get((ITableProperty) defaultProperty));
        } else if (defaultProperty instanceof InstanceProperty) {
            return getProperties().getProperty(property.getPropertyName(), instanceProperties.get((InstanceProperty) defaultProperty));
        } else {
            throw new RuntimeException("Unable to process SleeperProperty, should have either been null, an " +
                    "instance property or a table property");
        }
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
        set(TableProperty.SCHEMA, new SchemaSerDe().toJson(schema));
    }

    public void saveToS3(AmazonS3 s3Client) throws IOException {
        super.saveToS3(s3Client, instanceProperties.get(CONFIG_BUCKET), TABLES_PREFIX + "/" + get(TableProperty.TABLE_NAME));
        LOGGER.info("Saved table properties to bucket {}, key {}", instanceProperties.get(CONFIG_BUCKET), TABLES_PREFIX + "/" + get(TABLE_NAME));
    }

    public void loadFromS3(AmazonS3 s3Client, String tableName) throws IOException {
        LOGGER.info("Loading table properties from bucket {}, key {}", instanceProperties.get(CONFIG_BUCKET), TABLES_PREFIX + "/" + tableName);
        loadFromString(s3Client.getObjectAsString(instanceProperties.get(CONFIG_BUCKET), TABLES_PREFIX + "/" + tableName));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        TableProperties that = (TableProperties) o;

        return new EqualsBuilder()
                .appendSuper(super.equals(o))
                .append(instanceProperties, that.instanceProperties)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .appendSuper(super.hashCode())
                .append(instanceProperties)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .appendSuper(super.toString())
                .append("instanceProperties", instanceProperties)
                .toString();
    }
}
