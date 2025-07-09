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

import sleeper.core.row.Record;
import sleeper.core.schema.Schema;

import java.util.Collections;
import java.util.List;

/**
 * Filters out records older than a specified age. If the specified timestamp field is more than a certain length of
 * time ago then the record is removed. This is an example implementation of {@link SortedRecordIterator}.
 */
public class AgeOffIterator implements SortedRecordIterator {
    private String fieldName;
    private long ageOff;

    public AgeOffIterator() {
    }

    @Override
    public void init(String configString, Schema schema) {
        String[] fields = configString.split(",");
        if (2 != fields.length) {
            throw new IllegalArgumentException("Configuration string should have 2 fields: field name and age off time");
        }
        fieldName = fields[0];
        ageOff = Long.parseLong(fields[1]);
    }

    @Override
    public List<String> getRequiredValueFields() {
        return Collections.singletonList(fieldName);
    }

    @Override
    public CloseableIterator<Record> apply(CloseableIterator<Record> input) {
        return new FilteringIterator<>(input, record -> {
            Long value = (Long) record.get(fieldName);
            return null != value && System.currentTimeMillis() - value < ageOff;
        });
    }
}
