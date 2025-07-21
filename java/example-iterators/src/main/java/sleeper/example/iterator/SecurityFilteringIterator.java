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
package sleeper.example.iterator;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.SortedRowIterator;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Filters out records that a user is not allowed to see. This is intended to be used in a query, adding the user's
 * authorisations to the query in the iterator configuration. If the visibility field equals one of these authorisations
 * then the user is allowed to see the record. If the visibility field is the empty or null string then the user is also
 * allowed to see the record. This is an example implementation of {@link SortedRowIterator}.
 */
public class SecurityFilteringIterator implements SortedRowIterator {
    private String fieldName;
    private Set<String> auths;

    public SecurityFilteringIterator() {
    }

    @Override
    public void init(String configString, Schema schema) {
        String[] fields = configString.split(",");
        if (0 == fields.length) {
            throw new IllegalArgumentException("Configuration string should have at least 1 field: field name and (possibly empty) list of auths");
        }
        fieldName = fields[0];
        auths = new HashSet<>();
        auths.addAll(Arrays.asList(fields).subList(1, fields.length));
    }

    @Override
    public List<String> getRequiredValueFields() {
        return Collections.singletonList(fieldName);
    }

    @Override
    public CloseableIterator<Row> apply(CloseableIterator<Row> input) {
        return new SecurityFilteringIteratorInternal(input, fieldName, auths);
    }

    /**
     * Discards records in the input iterator where the security label is not one of the permitted auths.
     */
    public static class SecurityFilteringIteratorInternal implements CloseableIterator<Row> {
        private final CloseableIterator<Row> iterator;
        private final String fieldName;
        private final Set<String> auths;
        private Row next;

        public SecurityFilteringIteratorInternal(CloseableIterator<Row> iterator,
                String fieldName,
                Set<String> auths) {
            this.iterator = iterator;
            this.fieldName = fieldName;
            this.auths = auths;
            updateNextToAllowedValue();
        }

        private void updateNextToAllowedValue() {
            while (iterator.hasNext()) {
                next = iterator.next();
                if (allowed(next, fieldName, auths)) {
                    return;
                }
            }
            next = null;
        }

        @Override
        public boolean hasNext() {
            return null != next;
        }

        @Override
        public Row next() {
            Row toReturn = next;
            updateNextToAllowedValue();
            return toReturn;
        }

        @Override
        public void close() throws IOException {
            iterator.close();
        }
    }

    private static boolean allowed(Row row, String securityFieldname, Set<String> auths) {
        String securityLabel = (String) row.get(securityFieldname);
        if (null == securityLabel || 0 == securityLabel.length()) {
            return true;
        }
        return auths.contains(securityLabel);
    }
}
