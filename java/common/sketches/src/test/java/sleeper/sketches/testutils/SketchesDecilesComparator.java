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
package sleeper.sketches.testutils;

import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;

import static java.util.stream.Collectors.toMap;

public class SketchesDecilesComparator implements Comparator<SketchesDeciles> {

    private final Schema schema;
    private final Map<String, Comparator<Object>> comparatorByField;

    public SketchesDecilesComparator(Schema schema, Map<String, Comparator<Object>> comparatorByField) {
        this.schema = schema;
        this.comparatorByField = comparatorByField;
    }

    public static SketchesDecilesComparator longsMaxDiff(Schema schema, long maxDiff) {
        return new SketchesDecilesComparator(schema, schema.getRowKeyFields().stream()
                .filter(field -> field.getType() instanceof LongType)
                .collect(toMap(Field::getName, field -> new LongMaxDiffComparator(maxDiff))));
    }

    @Override
    public int compare(SketchesDeciles o1, SketchesDeciles o2) {
        for (Field field : schema.getRowKeyFields()) {
            SketchDeciles deciles1 = o1.getDecilesByField(field);
            SketchDeciles deciles2 = o2.getDecilesByField(field);
            Comparator<Object> comparator = comparatorByField.get(field.getName());
            if (comparator == null) {
                if (!Objects.equals(deciles1, deciles2)) {
                    return -1;
                }
            } else {
                int comparison = SketchDeciles.compare(deciles1, deciles2, comparator);
                if (comparison != 0) {
                    return comparison;
                }
            }
        }
        return 0;
    }

    private static class LongMaxDiffComparator implements Comparator<Object> {

        private final long maxDiff;

        LongMaxDiffComparator(long maxDiff) {
            this.maxDiff = maxDiff;
        }

        @Override
        public int compare(Object o1, Object o2) {
            long l1 = (long) o1;
            long l2 = (long) o2;
            long diff = l1 - l2;
            if (Math.abs(diff) <= maxDiff) {
                return 0;
            } else {
                return (int) diff;
            }
        }

    }

}
