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

package sleeper.systemtest.dsl.ingest;

import sleeper.core.row.Row;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.sourcedata.RowNumbers;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class DirectIngestDsl {

    private final SystemTestInstanceContext instance;
    private final DirectIngestDriver driver;
    private final Path tempDir;

    public DirectIngestDsl(SystemTestInstanceContext instance, DirectIngestDriver driver, Path tempDir) {
        this.instance = instance;
        this.driver = driver;
        this.tempDir = tempDir;
    }

    public DirectIngestDsl splitIngests(int numIngests, RowNumbers numbers) {
        if (numbers.numRows() % numIngests != 0) {
            throw new IllegalArgumentException("Number of ingests must split number of rows exactly");
        }
        int rowsPerIngest = numbers.numRows() / numIngests;
        List<FileReference> fileReferences = new ArrayList<>();
        IntStream.range(0, numIngests)
                .mapToObj(i -> numbers.range(i * rowsPerIngest, i * rowsPerIngest + rowsPerIngest))
                .map(range -> instance.numberedRows().iteratorFrom(range))
                .forEach(rows -> driver.ingest(tempDir, rows, fileReferences::addAll));
        AddFilesTransaction.fromReferences(fileReferences).synchronousCommit(instance.getStateStore());
        return this;
    }

    public DirectIngestDsl numberedRows(LongStream numbers) {
        driver.ingest(tempDir, instance.numberedRows().iteratorFrom(numbers));
        return this;
    }

    public void rows(Row... rows) {
        driver.ingest(tempDir, List.of(rows).iterator());
    }
}
