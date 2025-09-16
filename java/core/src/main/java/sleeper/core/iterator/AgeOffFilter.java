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
package sleeper.core.iterator;

import org.apache.commons.lang3.StringUtils;

import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * A filter to exclude records older than a given age.
 */
public class AgeOffFilter implements Predicate<Row> {
    private final String fieldName;
    private final long ageOff;

    public AgeOffFilter(String fieldName, long ageOff) {
        this.fieldName = fieldName;
        this.ageOff = ageOff;
    }

    /**
     * Parses a filtering configuration string.
     *
     * @param  configString the configuration string
     * @return              the filtering configuration
     */
    public static List<AgeOffFilter> parseConfig(String configString) {
        if (configString == null || configString.isEmpty()) {
            return List.of();
        }
        String[] filterParts = StringUtils.deleteWhitespace(configString).split("\\(");
        if ("ageoff".equalsIgnoreCase(filterParts[0])) {
            String[] filterInput = StringUtils.chop(filterParts[1]).split(","); //Chop to remove the trailing ')'
            return List.of(new AgeOffFilter(filterInput[0], Long.parseLong(filterInput[1])));
        } else {
            throw new IllegalArgumentException("Sleeper table filter not set to match ageOff(column,age), was: " + filterParts[0]);
        }
    }

    public String getFieldName() {
        return fieldName;
    }

    /**
     * Returns which value fields in the schema need to be read to apply this filter.
     *
     * @param  schema the schema
     * @return        the required value fields
     */
    public List<String> getRequiredValueFields(Schema schema) {
        if (schema.streamRowKeysThenSortKeys()
                .map(Field::getName)
                .noneMatch(key -> key.equals(fieldName))) {
            return List.of(fieldName);
        } else {
            return List.of();
        }
    }

    @Override
    public boolean test(Row row) {
        Long value = (Long) row.get(fieldName);
        return null != value && System.currentTimeMillis() - value < ageOff;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, ageOff);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof AgeOffFilter)) {
            return false;
        }
        AgeOffFilter other = (AgeOffFilter) obj;
        return Objects.equals(fieldName, other.fieldName) && ageOff == other.ageOff;
    }

    @Override
    public String toString() {
        return "AgeOffFilter{fieldName=" + fieldName + ", ageOff=" + ageOff + "}";
    }

}
