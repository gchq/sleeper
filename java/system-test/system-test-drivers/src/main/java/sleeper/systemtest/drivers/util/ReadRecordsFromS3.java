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

package sleeper.systemtest.drivers.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;

import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.parquet.record.ParquetReaderIterator;
import sleeper.parquet.record.ParquetRecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class ReadRecordsFromS3 {

    private ReadRecordsFromS3() {
    }

    public static Stream<Row> getRows(String bucketName, String objectKey, Schema schema, Configuration conf) {
        String path = "s3a://" + bucketName + "/" + objectKey;
        List<Row> rows = new ArrayList<>();
        try (ParquetReader<Row> reader = new ParquetRecordReader.Builder(new Path(path), schema)
                .withConf(conf).build();
                ParquetReaderIterator iterator = new ParquetReaderIterator(reader)) {
            iterator.forEachRemaining(rows::add);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return rows.stream();
    }
}
