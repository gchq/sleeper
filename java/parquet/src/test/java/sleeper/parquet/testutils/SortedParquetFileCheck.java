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
package sleeper.parquet.testutils;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;

import sleeper.core.record.testutils.SortedRecordsCheck;
import sleeper.core.schema.Schema;
import sleeper.parquet.record.ParquetReaderIterator;
import sleeper.parquet.record.RecordReadSupport;

import java.io.IOException;
import java.io.UncheckedIOException;

public class SortedParquetFileCheck {

    private SortedParquetFileCheck() {
    }

    public static SortedRecordsCheck check(Path path, Schema schema) {
        try (ParquetReaderIterator iterator = new ParquetReaderIterator(
                ParquetReader.builder(new RecordReadSupport(schema), path).build())) {
            return SortedRecordsCheck.check(schema, iterator);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
