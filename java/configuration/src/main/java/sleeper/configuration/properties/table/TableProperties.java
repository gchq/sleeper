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
package sleeper.configuration.properties.table;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.SleeperProperties;
import sleeper.configuration.properties.SleeperPropertiesValidationReporter;
import sleeper.configuration.properties.SleeperPropertyIndex;
import sleeper.configuration.properties.format.SleeperPropertiesPrettyPrinter;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;
import sleeper.core.table.TableStatus;

import java.io.PrintWriter;
import java.util.Optional;
import java.util.Properties;

import static sleeper.configuration.properties.PropertiesUtils.loadProperties;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.SCHEMA;
import static sleeper.configuration.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ONLINE;

/**
 * Contains values of the properties to configure a Sleeper table.
 */
public class TableProperties extends SleeperProperties<TableProperty> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableProperties.class);

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

    /**
     * Creates a copy of the given table properties.
     *
     * @param  tableProperties the table properties
     * @return                 the copy
     */
    public static TableProperties copyOf(TableProperties tableProperties) {
        InstanceProperties instanceProperties = InstanceProperties.copyOf(tableProperties.instanceProperties);
        return new TableProperties(instanceProperties, loadProperties(tableProperties.saveAsString()));
    }

    /**
     * Creates and validates an instance of this class with the given property values.
     *
     * @param  instanceProperties the instance properties
     * @param  properties         the property values
     * @return                    the table properties
     */
    public static TableProperties loadAndValidate(InstanceProperties instanceProperties, Properties properties) {
        TableProperties tableProperties = new TableProperties(instanceProperties, properties);
        tableProperties.validate();
        return tableProperties;
    }

    /**
     * Creates an instance with different property values than the given instance. Performs no validation, and uses the
     * same instance properties as the given instance.
     *
     * @param  tableProperties the original table properties
     * @param  newProperties   the property values
     * @return                 the new table properties
     */
    public static TableProperties reinitialise(TableProperties tableProperties, Properties newProperties) {
        return new TableProperties(tableProperties.instanceProperties, newProperties);
    }

    private static Schema loadSchema(Properties properties) {
        return Optional.ofNullable(properties.getProperty(SCHEMA.getPropertyName()))
                .map(Schema::loadFromString)
                .orElse(null);
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

        // This limit is based on calls to WriteTransactItems in DynamoDBFileReferenceStore.atomicallyUpdateX.
        // Also see the DynamoDB documentation:
        // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html
        if ("sleeper.statestore.dynamodb.DynamoDBStateStore".equals(get(STATESTORE_CLASSNAME))
                && getInt(COMPACTION_FILES_BATCH_SIZE) > 49) {
            LOGGER.warn("Detected a compaction batch size for this table which would be incompatible with the " +
                    "chosen statestore. Maximum value is 49.");
            reporter.invalidProperty(COMPACTION_FILES_BATCH_SIZE, get(COMPACTION_FILES_BATCH_SIZE));
        }
    }

    @Override
    public String get(TableProperty property) {
        return compute(property, value -> property.computeValue(value, instanceProperties, this));
    }

    public Schema getSchema() {
        return schema;
    }

    /**
     * Sets the schema of the Sleeper table.
     *
     * @param schema the schema
     */
    public void setSchema(Schema schema) {
        this.schema = schema;
        set(TableProperty.SCHEMA, new SchemaSerDe().toJson(schema));
    }

    @Override
    public SleeperPropertyIndex<TableProperty> getPropertiesIndex() {
        return TableProperty.Index.INSTANCE;
    }

    @Override
    protected SleeperPropertiesPrettyPrinter<TableProperty> getPrettyPrinter(PrintWriter writer) {
        return SleeperPropertiesPrettyPrinter.forTableProperties(writer);
    }

    public TableStatus getStatus() {
        return TableStatus.uniqueIdAndName(get(TABLE_ID), get(TABLE_NAME), getBoolean(TABLE_ONLINE));
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
}
