/*
 * Copyright 2022-2026 Crown Copyright
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

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.row.Row;
import sleeper.parquet.row.ParquetRowReaderFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class ReadRowsFromS3 {

    private ReadRowsFromS3() {
    }

    public static Stream<Row> getRows(String bucketName, String objectKey, Configuration conf) {
        String path = "s3a://" + bucketName + "/" + objectKey;
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = ParquetRowReaderFactory.parquetGenericRowIterator(new Path(path), conf)) {
            iterator.forEachRemaining(rows::add);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return rows.stream();
    }
}
