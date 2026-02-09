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

package sleeper.clients.report.filestatus;

import sleeper.core.statestore.FileReference;

import java.util.Collection;

/**
 * Statistics on the number of rows in a set of file references.
 */
public class FileRowsStats {

    private final long totalRows;
    private final long totalRowsApprox;
    private final long totalRowsKnown;

    private FileRowsStats(long totalRows, long totalRowsApprox, long totalRowsKnown) {
        this.totalRows = totalRows;
        this.totalRowsApprox = totalRowsApprox;
        this.totalRowsKnown = totalRowsKnown;
    }

    /**
     * Computes statistics on the number of rows.
     *
     * @param  references the file references
     * @return            the statistics
     */
    public static FileRowsStats from(Collection<FileReference> references) {
        long totalRowsApprox = 0;
        long totalRowsExact = 0;
        for (FileReference reference : references) {
            if (reference.isCountApproximate()) {
                totalRowsApprox += reference.getNumberOfRows();
            } else {
                totalRowsExact += reference.getNumberOfRows();
            }
        }
        return new FileRowsStats(totalRowsApprox + totalRowsExact, totalRowsApprox, totalRowsExact);
    }

    public long getTotalRows() {
        return totalRows;
    }

    public long getTotalRowsApprox() {
        return totalRowsApprox;
    }

    public long getTotalRowsKnown() {
        return totalRowsKnown;
    }
}
