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
package sleeper.core.record.testutils;

import sleeper.core.record.Record;
import sleeper.core.record.RecordComparator;
import sleeper.core.schema.Schema;

import java.util.Iterator;
import java.util.List;

public record SortedRecordsCheck(long recordsRead, List<Record> outOfOrder) {

    public static SortedRecordsCheck sorted(long count) {
        return new SortedRecordsCheck(count, List.of());
    }

    public static SortedRecordsCheck outOfOrderAt(long recordsRead, Record left, Record right) {
        return new SortedRecordsCheck(recordsRead, List.of(left, right));
    }

    public static SortedRecordsCheck check(Schema schema, Iterator<Record> records) {
        if (!records.hasNext()) {
            return sorted(0);
        }
        RecordComparator comparator = new RecordComparator(schema);
        Record record = records.next();
        long recordsRead = 1;
        while (records.hasNext()) {
            Record next = records.next();
            recordsRead++;
            int diff = comparator.compare(record, next);
            if (diff > 0) {
                return outOfOrderAt(recordsRead, record, next);
            }
            record = next;
        }
        return sorted(recordsRead);
    }

}
