/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.systemtest.drivers.python;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import sleeper.core.record.Record;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static sleeper.configuration.properties.instance.CommonProperty.ID;

public class PythonQueryDriver {
    private static final Gson GSON = new GsonBuilder().create();
    private final SleeperInstanceContext instance;
    private final PythonRunner pythonRunner;
    private final Path pythonDir;
    private final Path outputDir;

    public PythonQueryDriver(SleeperInstanceContext instance, Path pythonDir, Path outputDir) {
        this.instance = instance;
        this.pythonRunner = new PythonRunner(pythonDir);
        this.pythonDir = pythonDir;
        this.outputDir = outputDir;
    }

    public void exactKeys(String queryId, String keyName, List<String> keyValues) throws IOException, InterruptedException {
        pythonRunner.run(
                pythonDir.resolve("test/exact_query.py").toString(),
                "--instance", instance.getInstanceProperties().get(ID),
                "--table", instance.getTableName(),
                "--queryid", queryId,
                "--query", GSON.toJson(Map.of(keyName, keyValues)),
                "--outdir", outputDir.toString());
    }

    public void range(String queryId, String key, Object min, Object max) throws IOException, InterruptedException {
        pythonRunner.run(
                pythonDir.resolve("test/range_query.py").toString(),
                "--instance", instance.getInstanceProperties().get(ID),
                "--table", instance.getTableName(),
                "--queryid", queryId,
                "--query", GSON.toJson(Map.of(key, List.of(min, max))),
                "--outdir", outputDir.toString());
    }

    public void range(String queryId, String key, Object min, boolean minInclusive, Object max, boolean maxInclusve)
            throws IOException, InterruptedException {
        pythonRunner.run(
                pythonDir.resolve("test/range_query.py").toString(),
                "--instance", instance.getInstanceProperties().get(ID),
                "--table", instance.getTableName(),
                "--queryid", queryId,
                "--query", GSON.toJson(Map.of(key, List.of(min, minInclusive, max, maxInclusve))),
                "--outdir", outputDir.toString());
    }

    public Stream<Record> results(String queryId) {
        String path = "file://" + outputDir.resolve(queryId + ".txt");
        List<Record> records = new ArrayList<>();
        try {
            ParquetRecordReader reader = new ParquetRecordReader(new org.apache.hadoop.fs.Path(path),
                    instance.getTableProperties().getSchema());

            Record record = reader.read();
            while (null != record) {
                records.add(new Record(record));
                record = reader.read();
            }
            reader.close();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return records.stream();
    }
}
