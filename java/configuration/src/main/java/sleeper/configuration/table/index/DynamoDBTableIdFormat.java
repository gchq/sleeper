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

package sleeper.configuration.table.index;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import sleeper.core.table.TableStatus;

import java.util.Map;

import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getStringAttribute;

/**
 * Reads and writes DynamoDB items in the Sleeper table index. Converts to and from {@link TableStatus}.
 */
class DynamoDBTableIdFormat {
    private DynamoDBTableIdFormat() {
    }

    static final String TABLE_NAME_FIELD = "TableName";
    static final String TABLE_ID_FIELD = "TableId";
    static final String ONLINE_FIELD = "Online";

    public static Map<String, AttributeValue> getItem(TableStatus table) {
        return Map.of(
                TABLE_ID_FIELD, createStringAttribute(table.getTableUniqueId()),
                TABLE_NAME_FIELD, createStringAttribute(table.getTableName()),
                ONLINE_FIELD, createStringAttribute(Boolean.toString(table.isOnline())));
    }

    public static Map<String, AttributeValue> getIdKey(TableStatus table) {
        return Map.of(TABLE_ID_FIELD, createStringAttribute(table.getTableUniqueId()));
    }

    public static Map<String, AttributeValue> getNameKey(TableStatus table) {
        return Map.of(TABLE_NAME_FIELD, createStringAttribute(table.getTableName()));
    }

    public static Map<String, AttributeValue> getOnlineKey(TableStatus table) {
        return Map.of(
                TABLE_NAME_FIELD, createStringAttribute(table.getTableName()),
                ONLINE_FIELD, createStringAttribute(Boolean.toString(table.isOnline())));
    }

    public static TableStatus readItem(Map<String, AttributeValue> item) {
        return TableStatus.uniqueIdAndName(
                getStringAttribute(item, TABLE_ID_FIELD),
                getStringAttribute(item, TABLE_NAME_FIELD),
                getStringAttribute(item, ONLINE_FIELD).equals("true"));
    }
}
