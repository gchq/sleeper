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
package sleeper.core.tracker.job.run;

import java.util.Objects;

/**
 * A data structure for storing the number of rows read and written.
 */
public class RowsProcessed {

    public static final RowsProcessed NONE = new RowsProcessed(0, 0);

    private final long rowsRead;
    private final long rowsWritten;

    public RowsProcessed(long rowsRead, long rowsWritten) {
        this.rowsRead = rowsRead;
        this.rowsWritten = rowsWritten;
    }

    public long getRowsRead() {
        return rowsRead;
    }

    public long getRowsWritten() {
        return rowsWritten;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowsProcessed that = (RowsProcessed) o;
        return rowsRead == that.rowsRead && rowsWritten == that.rowsWritten;
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowsRead, rowsWritten);
    }

    @Override
    public String toString() {
        return "RowsProcessed{" +
                "rowsRead=" + rowsRead +
                ", rowsWritten=" + rowsWritten +
                '}';
    }
}
