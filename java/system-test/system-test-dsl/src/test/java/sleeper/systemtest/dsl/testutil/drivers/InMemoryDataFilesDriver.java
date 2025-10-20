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
package sleeper.systemtest.dsl.testutil.drivers;

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.row.Row;
import sleeper.core.row.testutils.InMemoryRowStore;
import sleeper.core.schema.Schema;
import sleeper.core.table.TableFilePaths;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.instance.DataFilesDriver;
import sleeper.systemtest.dsl.util.DataFileDuplication;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

public class InMemoryDataFilesDriver implements DataFilesDriver {

    private final InMemoryRowStore data;
    private final Supplier<String> filenameSupplier;

    public InMemoryDataFilesDriver(InMemoryRowStore data, SystemTestContext context) {
        this(data, filenameSupplier(context));
    }

    public InMemoryDataFilesDriver(InMemoryRowStore data, Supplier<String> filenameSupplier) {
        this.data = data;
        this.filenameSupplier = filenameSupplier;
    }

    private static Supplier<String> filenameSupplier(SystemTestContext context) {
        return () -> {
            TableFilePaths paths = TableFilePaths.buildDataFilePathPrefix(context.instance().getInstanceProperties(), context.instance().getTableProperties());
            return paths.constructPartitionParquetFilePath("duplicate", UUID.randomUUID().toString());
        };
    }

    @Override
    public CloseableIterator<Row> getRows(Schema schema, String filename) {
        return data.openFile(filename);
    }

    @Override
    public List<DataFileDuplication> duplicateFiles(int times, Collection<String> files) {
        List<DataFileDuplication> results = new ArrayList<>();
        for (String originalFilename : files) {
            List<Row> rows = data.getRowsOrThrow(originalFilename);
            List<String> duplicateFilenames = new ArrayList<>(times);
            for (int i = 0; i < times; i++) {
                String duplicateFilename = filenameSupplier.get();
                data.addFile(duplicateFilename, rows);
                duplicateFilenames.add(duplicateFilename);
            }
            results.add(new DataFileDuplication(originalFilename, duplicateFilenames));
        }
        return results;
    }

}
