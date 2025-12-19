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
package sleeper.bulkimport.runner.common;

import org.apache.spark.sql.Row;

import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.MapType;

import java.util.List;

/**
 * Converts between Spark rows and Sleeper rows.
 */
public class SparkRowMapper {

    private final List<Field> allSchemaFields;

    public SparkRowMapper(Schema schema) {
        this.allSchemaFields = schema.getAllFields();
    }

    /**
     * Converts from a Spark row to a Sleeper row.
     *
     * @param  row the Spark row
     * @return     the same row, as the Sleeper row type
     */
    public sleeper.core.row.Row toSleeperRow(Row row) {
        sleeper.core.row.Row outRow = new sleeper.core.row.Row();
        int i = 0;
        for (Field field : allSchemaFields) {
            if (field.getType() instanceof ListType) {
                outRow.put(field.getName(), row.getList(i));
            } else if (field.getType() instanceof MapType) {
                outRow.put(field.getName(), row.getJavaMap(i));
            } else {
                outRow.put(field.getName(), row.get(i));
            }
            i++;
        }
        return outRow;
    }

}
