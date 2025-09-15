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

import sleeper.core.row.Row;

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

    public String getFieldName() {
        return fieldName;
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
