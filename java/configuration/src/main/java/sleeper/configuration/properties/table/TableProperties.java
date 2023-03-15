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
package sleeper.configuration.properties.table;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.InstanceProperty;
import sleeper.configuration.properties.SleeperProperties;
import sleeper.configuration.properties.SleeperPropertiesValidationReporter;
import sleeper.configuration.properties.SleeperProperty;
import sleeper.configuration.properties.SleeperPropertyIndex;
import sleeper.configuration.properties.format.SleeperPropertiesPrettyPrinter;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.SCHEMA;
import static sleeper.configuration.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class TableProperties extends SleeperProperties<TableProperty> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableProperties.class);

    public static final String TABLES_PREFIX = "tables";
    // Schema is cached for faster access
    private Schema schema;
    private final InstanceProperties instanceProperties; // Used for default properties

    public TableProperties(InstanceProperties instanceProperties) {
        this.instanceProperties = instanceProperties;
    }

    public TableProperties(InstanceProperties instanceProperties, Properties properties) {
        super(properties);
        this.instanceProperties = instanceProperties;
        schema = loadSchema(properties);
    }

    public static TableProperties loadAndValidate(InstanceProperties instanceProperties, Properties properties) {
        TableProperties tableProperties = new TableProperties(instanceProperties, properties);
        tableProperties.validate();
        return tableProperties;
    }

    public static TableProperties reinitialise(TableProperties tableProperties, Properties newProperties) {
        return new TableProperties(tableProperties.instanceProperties, newProperties);
    }

    private static Schema loadSchema(Properties properties) {
        return Schema.loadFromString(
                Objects.requireNonNull(properties.getProperty(SCHEMA.getPropertyName()),
                        "Schema not set in property " + SCHEMA.getPropertyName()));
    }

    @Override
    protected void init() {
        String schemaProperty = get(TableProperty.SCHEMA);
        if (schemaProperty != null) {
            schema = Schema.loadFromString(schemaProperty);
        }
        super.init();
    }

    @Override
    public void validate(SleeperPropertiesValidationReporter reporter) {
        super.validate(reporter);

        // This limit is based on calls to WriteTransactItems in DynamoDBFileInfoStore.atomicallyUpdateX.
        // Also see the DynamoDB documentation:
        // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html
        if ("sleeper.statestore.dynamodb.DynamoDBStateStore".equals(get(STATESTORE_CLASSNAME))
                && getInt(COMPACTION_FILES_BATCH_SIZE) > 48) {
            LOGGER.warn("Detected a compaction batch size for this table which would be incompatible with the " +
                    "chosen statestore. Maximum value is 48.");
            reporter.invalidProperty(COMPACTION_FILES_BATCH_SIZE, get(COMPACTION_FILES_BATCH_SIZE));
        }
    }

    @Override
    public String get(TableProperty property) {
        SleeperProperty defaultProperty = property.getDefaultProperty();
        if (defaultProperty == null) {
            return super.get(property);
        } else if (defaultProperty instanceof TableProperty) {
            return getProperties().getProperty(property.getPropertyName(), get((TableProperty) defaultProperty));
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
        LOGGER.info("Loading table properties from bucket {}, key {}/{}", instanceProperties.get(CONFIG_BUCKET), TABLES_PREFIX, tableName);
        loadFromString(s3Client.getObjectAsString(instanceProperties.get(CONFIG_BUCKET), TABLES_PREFIX + "/" + tableName));
    }

    @Override
    public SleeperPropertyIndex<TableProperty> getPropertiesIndex() {
        return TableProperty.Index.INSTANCE;
    }

    @Override
    protected SleeperPropertiesPrettyPrinter<TableProperty> getPrettyPrinter(PrintWriter writer) {
        return SleeperPropertiesPrettyPrinter.forTableProperties(writer);
    }

    public static Stream<TableProperties> streamTablesFromS3(AmazonS3 s3, InstanceProperties instanceProperties) {
        Iterable<S3ObjectSummary> objects = S3Objects.withPrefix(
                s3, instanceProperties.get(CONFIG_BUCKET), "tables/");
        return StreamSupport.stream(objects.spliterator(), false)
                .map(tableConfigObject -> {
                    try {
                        return loadTableFromS3(s3, instanceProperties, tableConfigObject);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
    }

    private static TableProperties loadTableFromS3(
            AmazonS3 s3, InstanceProperties instanceProperties, S3ObjectSummary tableConfigObject) throws IOException {
        TableProperties tableProperties = new TableProperties(instanceProperties);
        try (InputStream in = s3.getObject(
                        tableConfigObject.getBucketName(),
                        tableConfigObject.getKey())
                .getObjectContent()) {
            tableProperties.load(in);
        }
        return tableProperties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

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
