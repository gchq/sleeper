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

package sleeper.clients.status.report.filestatus;

import sleeper.core.statestore.FileReference;

import java.util.Collection;

public class FileRecordsStats {

    private final long totalRecords;
    private final long totalRecordsApprox;
    private final long totalRecordsKnown;

    private FileRecordsStats(long totalRecords, long totalRecordsApprox, long totalRecordsKnown) {
        this.totalRecords = totalRecords;
        this.totalRecordsApprox = totalRecordsApprox;
        this.totalRecordsKnown = totalRecordsKnown;
    }

    public static FileRecordsStats from(Collection<FileReference> references) {
        long totalRecordsApprox = 0;
        long totalRecordsExact = 0;
        for (FileReference reference : references) {
            if (reference.isCountApproximate()) {
                totalRecordsApprox += reference.getNumberOfRecords();
            } else {
                totalRecordsExact += reference.getNumberOfRecords();
            }
        }
        return new FileRecordsStats(totalRecordsApprox + totalRecordsExact, totalRecordsApprox, totalRecordsExact);
    }

    public long getTotalRecords() {
        return totalRecords;
    }

    public long getTotalRecordsApprox() {
        return totalRecordsApprox;
    }

    public long getTotalRecordsKnown() {
        return totalRecordsKnown;
    }
}
