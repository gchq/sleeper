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
package sleeper.systemtest.drivers.instance;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.parquet.record.ParquetReaderIterator;
import sleeper.parquet.record.RecordReadSupport;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.instance.DataFilesDriver;

import java.io.IOException;
import java.io.UncheckedIOException;

public class AwsDataFilesDriver implements DataFilesDriver {

    private Configuration hadoopConf;

    public AwsDataFilesDriver(SystemTestClients clients) {
        hadoopConf = clients.createHadoopConf();
    }

    @Override
    public CloseableIterator<Row> getRows(Schema schema, String filename) {
        try {
            return new ParquetReaderIterator(
                    ParquetReader.builder(new RecordReadSupport(schema), new Path(filename))
                            .withConf(hadoopConf)
                            .build());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
