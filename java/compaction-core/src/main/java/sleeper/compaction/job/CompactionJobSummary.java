/*
 * Copyright 2022 Crown Copyright
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
package sleeper.compaction.job;

import java.util.Objects;

public class CompactionJobSummary {
    private final long linesRead;
    private final long linesWritten;

    public CompactionJobSummary(long linesRead, long linesWritten) {
        this.linesRead = linesRead;
        this.linesWritten = linesWritten;
    }

    public long getLinesRead() {
        return linesRead;
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
        CompactionJobSummary that = (CompactionJobSummary) o;
        return linesRead == that.linesRead && linesWritten == that.linesWritten;
    }

    @Override
    public int hashCode() {
        return Objects.hash(linesRead, linesWritten);
    }
}
