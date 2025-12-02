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
package sleeper.core.row.testutils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.row.Row;
import sleeper.core.row.RowComparator;
import sleeper.core.schema.Schema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

/**
 * Checks whether a given set of rows are sorted. Represents the results of a check.
 *
 * @param rowsRead   the number of rows that were read during the check
 * @param outOfOrder the first rows that were found out of order
 */
public record SortedRowsCheck(long rowsRead, List<Row> outOfOrder) {

    public static final Logger LOGGER = LoggerFactory.getLogger(SortedRowsCheck.class);

    /**
     * Checks whether a given set of rows are sorted. Will read through all rows and close the given iterator.
     *
     * @param  schema the schema of the rows
     * @param  rows   the rows
     * @return        the results of the check
     */
    public static SortedRowsCheck check(Schema schema, CloseableIterator<Row> rows) {
        try (rows) {
            LOGGER.info("Checking whether rows are sorted, reading...");
            if (!rows.hasNext()) {
                return sorted(0);
            }
            RowComparator comparator = new RowComparator(schema);
            Row row = rows.next();
            long rowsRead = 1;
            while (rows.hasNext()) {
                Row next = rows.next();
                rowsRead++;
                if (rowsRead % 10_000_000 == 0) {
                    LOGGER.info("Read {} rows", rowsRead);
                }
                int diff = comparator.compare(row, next);
                if (diff > 0) {
                    LOGGER.info("Found rows out of order after reading {}", rowsRead);
                    return outOfOrderAt(rowsRead, row, next);
                }
                row = next;
            }
            LOGGER.info("Found rows fully sorted after reading {}", rowsRead);
            return sorted(rowsRead);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Creates the results of a check reporting that rows were sorted.
     *
     * @param  rowsRead the number of rows that were read
     * @return          the check result
     */
    public static SortedRowsCheck sorted(long rowsRead) {
        return new SortedRowsCheck(rowsRead, List.of());
    }

    /**
     * Creates the results of a check reporting that some rows were not sorted.
     *
     * @param  rowsRead the number of rows that were read
     * @param  left     the row that appeared first, but should have been later
     * @param  right    the row that appeared second, but should have been earlier
     * @return          the check result
     */
    public static SortedRowsCheck outOfOrderAt(long rowsRead, Row left, Row right) {
        return new SortedRowsCheck(rowsRead, List.of(left, right));
    }

}
