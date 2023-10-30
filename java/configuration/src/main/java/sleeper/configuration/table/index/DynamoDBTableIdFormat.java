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

package sleeper.configuration.table.index;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import sleeper.core.table.TableIdentity;

import java.util.Map;

import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getStringAttribute;

class DynamoDBTableIdFormat {
    private DynamoDBTableIdFormat() {
    }

    static final String TABLE_NAME_FIELD = "TableName";
    static final String TABLE_ID_FIELD = "TableId";

    public static Map<String, AttributeValue> getItem(TableIdentity id) {
        return Map.of(
                TABLE_ID_FIELD, createStringAttribute(id.getTableUniqueId()),
                TABLE_NAME_FIELD, createStringAttribute(id.getTableName()));
    }

    public static Map<String, AttributeValue> getIdKey(TableIdentity id) {
        return Map.of(TABLE_ID_FIELD, createStringAttribute(id.getTableUniqueId()));
    }

    public static Map<String, AttributeValue> getNameKey(TableIdentity id) {
        return Map.of(TABLE_NAME_FIELD, createStringAttribute(id.getTableName()));
    }

    public static TableIdentity readItem(Map<String, AttributeValue> item) {
        return TableIdentity.uniqueIdAndName(
                getStringAttribute(item, TABLE_ID_FIELD),
                getStringAttribute(item, TABLE_NAME_FIELD));
    }
}
