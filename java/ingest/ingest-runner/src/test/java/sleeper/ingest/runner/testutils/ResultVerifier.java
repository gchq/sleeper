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
package sleeper.ingest.runner.testutils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.parquet.record.ParquetReaderIterator;
import sleeper.parquet.record.ParquetRowReader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ResultVerifier {

    private ResultVerifier() {
    }

    public static List<Row> readMergedRowsFromPartitionDataFiles(Schema sleeperSchema,
            List<FileReference> fileReferenceList,
            Configuration hadoopConfiguration) {
        List<Row> rowsRead = new ArrayList<>();
        Set<String> filenames = new HashSet<>();
        for (FileReference fileReference : fileReferenceList) {
            if (filenames.contains(fileReference.getFilename())) {
                continue;
            }
            filenames.add(fileReference.getFilename());
            try (CloseableIterator<Row> iterator = createParquetReaderIterator(
                    sleeperSchema, new Path(fileReference.getFilename()), hadoopConfiguration)) {
                iterator.forEachRemaining(rowsRead::add);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return rowsRead;
    }

    public static List<Row> readRowsFromPartitionDataFile(Schema sleeperSchema,
            FileReference fileReference,
            Configuration hadoopConfiguration) {

        try (CloseableIterator<Row> iterator = createParquetReaderIterator(
                sleeperSchema, new Path(fileReference.getFilename()), hadoopConfiguration)) {
            List<Row> rowsRead = new ArrayList<>();
            iterator.forEachRemaining(rowsRead::add);
            return rowsRead;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static ParquetReaderIterator createParquetReaderIterator(Schema sleeperSchema,
            Path filePath,
            Configuration hadoopConfiguration) {
        try {
            ParquetReader<Row> recordParquetReader = new ParquetRowReader.Builder(filePath, sleeperSchema)
                    .withConf(hadoopConfiguration)
                    .build();
            return new ParquetReaderIterator(recordParquetReader);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
