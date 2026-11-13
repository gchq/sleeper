/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.parquet.row;

import org.apache.avro.NameValidator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Reads Avro generic records from a Parquet file, using the schema held in the file. Avro can only represent a map
 * if its keys are strings, but a Sleeper table can hold maps with other types of key. Those maps are read as a list
 * of records that each hold a key and a value. See {@link AvroRowConverter} for how they are converted back to a map.
 */
class GenericRowReadSupport extends AvroReadSupport<GenericRecord> {
    private MessageType fileSchema;

    GenericRowReadSupport() {
        super(GenericData.get());
    }

    @Override
    public ReadContext init(InitContext context) {
        fileSchema = context.getFileSchema();
        ReadContext avroContext = withoutAvroNameValidation(() -> super.init(context));
        MessageType requested = avroContext.getRequestedSchema();
        MessageType readSchema = new MessageType(requested.getName(),
                requested.getFields().stream().map(GenericRowReadSupport::readMapsWithNonStringKeysAsLists).toList());
        return new ReadContext(readSchema, avroContext.getReadSupportMetadata());
    }

    @Override
    public RecordMaterializer<GenericRecord> prepareForRead(
            ParquetConfiguration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema, ReadContext readContext) {
        return withoutAvroNameValidation(() -> super.prepareForRead(configuration, keyValueMetaData, fileSchema, readContext));
    }

    // Runs an operation that builds an Avro schema, without validating field names. Avro only allows letters, numbers
    // and underscores in a field name, but a Parquet file can hold any field name, e.g. "count(*)" for a field
    // computed by an SQL query. The setting is held per thread in Avro, and is restored afterwards.
    private static <T> T withoutAvroNameValidation(Supplier<T> operation) {
        NameValidator validatorBefore = Schema.getNameValidator();
        Schema.setNameValidator(NameValidator.NO_VALIDATION);
        try {
            return operation.get();
        } finally {
            Schema.setNameValidator(validatorBefore);
        }
    }

    /**
     * Retrieves the schema of the file being read. This is only available once the first record has been read.
     *
     * @return the Parquet schema held in the file
     */
    MessageType getFileSchema() {
        return fileSchema;
    }

    private static Type readMapsWithNonStringKeysAsLists(Type type) {
        if (type.isPrimitive()) {
            return type;
        }
        GroupType group = type.asGroupType();
        List<Type> fields = group.getFields().stream().map(GenericRowReadSupport::readMapsWithNonStringKeysAsLists).toList();
        if (!isMapWithNonStringKey(group)) {
            return group.withNewFields(fields);
        }
        GroupType keyValue = fields.get(0).asGroupType();
        return Types.buildGroup(group.getRepetition())
                .as(LogicalTypeAnnotation.listType())
                .addField(Types.repeatedGroup().addFields(keyValue.getFields().toArray(Type[]::new)).named(keyValue.getName()))
                .named(group.getName());
    }

    /**
     * Checks whether a Parquet type is a map that cannot be represented in Avro because its keys are not strings.
     *
     * @param  type the Parquet type
     * @return      true if the type is a map with keys that are not strings
     */
    static boolean isMapWithNonStringKey(Type type) {
        if (!isMap(type)) {
            return false;
        }
        Type key = type.asGroupType().getType(0).asGroupType().getType(0);
        return !(key.isPrimitive()
                && key.asPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.BINARY
                && LogicalTypeAnnotation.stringType().equals(key.getLogicalTypeAnnotation()));
    }

    /**
     * Checks whether a Parquet type is a map, holding a repeated group with a key and a value.
     *
     * @param  type the Parquet type
     * @return      true if the type is a map
     */
    static boolean isMap(Type type) {
        if (type.isPrimitive() || !(type.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation)) {
            return false;
        }
        GroupType group = type.asGroupType();
        if (group.getFieldCount() != 1 || group.getType(0).isPrimitive() || !group.getType(0).isRepetition(Type.Repetition.REPEATED)) {
            return false;
        }
        return group.getType(0).asGroupType().getFieldCount() == 2;
    }
}
