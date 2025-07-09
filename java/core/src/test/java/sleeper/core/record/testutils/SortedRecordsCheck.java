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

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.record.Record;
import sleeper.core.record.RecordComparator;
import sleeper.core.schema.Schema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

/**
 * Checks whether a given set of records are sorted. Represents the results of a check.
 *
 * @param recordsRead the number of records that were read during the check
 * @param outOfOrder  the first records that were found out of order
 */
public record SortedRecordsCheck(long recordsRead, List<Record> outOfOrder) {

    /**
     * Checks whether a given set of records are sorted. Will read through all records and close the given iterator.
     *
     * @param  schema  the schema of the records
     * @param  records the records
     * @return         the results of the check
     */
    public static SortedRecordsCheck check(Schema schema, CloseableIterator<Record> records) {
        try (records) {
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
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Creates the results of a check reporting that records were sorted.
     *
     * @param  count the number of records
     * @return       the check result
     */
    public static SortedRecordsCheck sorted(long count) {
        return new SortedRecordsCheck(count, List.of());
    }

    /**
     * Creates the results of a check reporting that some records were not sorted.
     *
     * @param  recordsRead the number of records that were read
     * @param  left        the record that appeared first, but should have been later
     * @param  right       the record that appeared second, but should have been earlier
     * @return             the check result
     */
    public static SortedRecordsCheck outOfOrderAt(long recordsRead, Record left, Record right) {
        return new SortedRecordsCheck(recordsRead, List.of(left, right));
    }

}
