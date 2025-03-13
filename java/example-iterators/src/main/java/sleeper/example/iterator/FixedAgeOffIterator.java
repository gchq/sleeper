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
import sleeper.core.iterator.FilteringIterator;
import sleeper.core.iterator.SortedRecordIterator;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;

import java.util.List;

/**
 * Filter to exclude records from before a certain date. This is an example implementation of
 * {@link SortedRecordIterator}.
 */
public class FixedAgeOffIterator implements SortedRecordIterator {
    private String fieldName;
    private long minValue;

    @Override
    public CloseableIterator<Record> apply(CloseableIterator<Record> input) {
        return new FilteringIterator<>(input, record -> {
            Long value = (Long) record.get(fieldName);
            return null != value && value >= minValue;
        });
    }

    @Override
    public void init(String configString, Schema schema) {
        String[] fields = configString.split(",");
        if (2 != fields.length) {
            throw new IllegalArgumentException("Configuration string should have 2 fields: field name and minimum value");
        }
        fieldName = fields[0];
        minValue = Long.parseLong(fields[1]);
    }

    @Override
    public List<String> getRequiredValueFields() {
        return List.of(fieldName);
    }

}
