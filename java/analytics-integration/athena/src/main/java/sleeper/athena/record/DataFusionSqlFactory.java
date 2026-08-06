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
package sleeper.athena.record;

import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;

import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Translates the predicates of an Athena query into an SQL query applied to the DataFusion query. Row-key
 * predicates are handled separately as regions.
 * <p>
 * Any predicate that cannot be translated is simply omitted --- it wil be applied by Athena itself. Note tha
 * SQL comparisons and IN lists exclude nulls, so whenever a value set permits nulls the condition is widened
 * with an explicit IS NULL term. String, int and long value fields are translated; other types (e.g. byte
 * arrays and complex types) are left for Athena.
 */
public class DataFusionSqlFactory {
    private final Schema schema;

    public DataFusionSqlFactory(Schema schema) {
        this.schema = schema;
    }

    /**
     * Builds a DataFusion SQL query from the Athena constraints. If no predicate can be converted into SQL
     * then null is returned.
     *
     * @param  constraints the Athena constraint summary (a map of field name to value set)
     * @return             a SQL query selecting from the query_results alias, or null
     */
    public String toSql(Map<String, ValueSet> constraints) {
        if (constraints == null || constraints.isEmpty()) {
            return null;
        }
        List<String> conditions = new ArrayList<>();
        for (Field field : schema.getValueFields()) {
            ValueSet valueSet = constraints.get(field.getName());
            if (valueSet == null) {
                continue;
            }
            String condition = fieldCondition(field, valueSet);
            if (condition != null) {
                conditions.add(condition);
            }
        }
        if (conditions.isEmpty()) {
            return null;
        }
        return "SELECT * FROM query_results WHERE " + String.join(" AND ", conditions) + ";";
    }

    private String fieldCondition(Field field, ValueSet valueSet) {
        Type type = field.getType();
        if (!(type instanceof StringType || type instanceof IntType || type instanceof LongType)) {
            return null;
        }
        String expression;
        if (valueSet instanceof SortedRangeSet) {
            expression = rangeCondition(field, (SortedRangeSet) valueSet);
        } else if (valueSet instanceof EquatableValueSet) {
            expression = equalityCondition(field, (EquatableValueSet) valueSet);
        } else {
            return null;
        }
        if (expression == null) {
            return null;
        }
        // Comparisons and IN lists never match nulls, so widen the condition when the predicate allows them.
        if (valueSet.isNullAllowed()) {
            return "(" + expression + " OR " + column(field) + " IS NULL)";
        }
        return expression;
    }

    private String rangeCondition(Field field, SortedRangeSet valueSet) {
        String column = column(field);
        List<String> orTerms = new ArrayList<>();
        for (com.amazonaws.athena.connector.lambda.domain.predicate.Range range : valueSet.getOrderedRanges()) {
            List<String> bounds = new ArrayList<>();
            if (!range.getLow().isLowerUnbounded()) {
                String operator = range.getLow().getBound() == Marker.Bound.EXACTLY ? ">=" : ">";
                bounds.add(column + " " + operator + " " + literal(field.getType(), range.getLow().getValue()));
            }
            if (!range.getHigh().isUpperUnbounded()) {
                String operator = range.getHigh().getBound() == Marker.Bound.EXACTLY ? "<=" : "<";
                bounds.add(column + " " + operator + " " + literal(field.getType(), range.getHigh().getValue()));
            }
            if (bounds.isEmpty()) {
                // The range has no bounds so there is no constraint to apply.
                return null;
            }
            orTerms.add(bounds.size() == 1 ? bounds.get(0) : "(" + String.join(" AND ", bounds) + ")");
        }
        if (orTerms.isEmpty()) {
            return null;
        }
        return orTerms.size() == 1 ? orTerms.get(0) : "(" + String.join(" OR ", orTerms) + ")";
    }

    private String equalityCondition(Field field, EquatableValueSet valueSet) {
        int count = valueSet.getValues().getRowCount();
        if (count == 0) {
            return null;
        }
        String column = column(field);
        List<String> literals = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            literals.add(literal(field.getType(), valueSet.getValue(i)));
        }
        if (valueSet.isWhiteList()) {
            if (count == 1) {
                return column + " = " + literals.get(0);
            }
            return column + " IN (" + String.join(", ", literals) + ")";
        } else {
            if (count == 1) {
                return column + " <> " + literals.get(0);
            }
            return column + " NOT IN (" + String.join(", ", literals) + ")";
        }
    }

    private static String column(Field field) {
        return "\"" + field.getName().replace("\"", "\"\"") + "\"";
    }

    private static String literal(Type type, Object value) {
        if (type instanceof StringType) {
            return "'" + value.toString().replace("'", "''") + "'";
        } else if (type instanceof IntType) {
            return Integer.toString(((Number) value).intValue());
        } else if (type instanceof LongType) {
            return Long.toString(((Number) value).longValue());
        } else {
            throw new IllegalArgumentException("Unexpected value field type: " + type);
        }
    }
}
