/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.core.record.process;

import java.util.Objects;

public class RecordsProcessed {
    private final long recordsRead;
    private final long linesWritten;

    public RecordsProcessed(long recordsRead, long linesWritten) {
        this.recordsRead = recordsRead;
        this.linesWritten = linesWritten;
    }

    public long getRecordsRead() {
        return recordsRead;
    }

    public long getLinesWritten() {
        return linesWritten;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RecordsProcessed that = (RecordsProcessed) o;
        return recordsRead == that.recordsRead && linesWritten == that.linesWritten;
    }

    @Override
    public int hashCode() {
        return Objects.hash(recordsRead, linesWritten);
    }

    @Override
    public String toString() {
        return "CompactionJobRecordsProcessed{" +
                "recordsRead=" + recordsRead +
                ", linesWritten=" + linesWritten +
                '}';
    }
}
