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

import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import sleeper.core.schema.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * Checks Parquet schema compatibility for standard ingest without reading row data.
 */
public class ParquetSchemaValidation {
    private ParquetSchemaValidation() {
    }

    /**
     * Checks the reader projection and the presence of non-nullable fields. Missing nullable fields and extra file
     * columns are allowed. This does not validate actual values, page integrity, or Spark bulk import compatibility.
     *
     * @param  schema     the target Sleeper table schema
     * @param  fileSchema the schema from the Parquet footer
     * @return            descriptions of incompatible fields, or an empty list if compatible
     */
    public static List<String> validate(Schema schema, MessageType fileSchema) {
        MessageType requested = SchemaConverter.getSchema(schema);
        List<String> problems = new ArrayList<>();
        for (Type field : requested.getFields()) {
            if (!fileSchema.containsField(field.getName())) {
                if (field.isRepetition(Type.Repetition.REQUIRED)) {
                    problems.add("Missing non-nullable field '" + field.getName() + "'.");
                }
                continue;
            }
            Type fileField = fileSchema.getType(field.getName());
            if (field.isRepetition(Type.Repetition.REQUIRED) && fileField.isRepetition(Type.Repetition.OPTIONAL)) {
                problems.add("Field '" + field.getName() + "' is nullable in the file but non-nullable in the table.");
                continue;
            }
            try {
                new ColumnIOFactory().getColumnIO(new MessageType("requested", field), new MessageType("file", fileField), true);
            } catch (ParquetDecodingException e) {
                problems.add("Field '" + field.getName() + "': " + e.getMessage());
            }
        }
        return problems;
    }
}
