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
package sleeper.clients.table.partition;

import org.apache.hadoop.conf.Configuration;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.ConcatenatingIterator;
import sleeper.core.iterator.LimitingIterator;
import sleeper.core.record.Row;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;
import sleeper.parquet.record.ParquetReaderIterator;
import sleeper.parquet.record.ParquetRecordReader;
import sleeper.parquet.utils.HadoopConfigurationProvider;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.core.properties.local.WriteSplitPoints.writeSplitPoints;

public class EstimateSplitPointsClient {

    private EstimateSplitPointsClient() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 6) {
            throw new IllegalArgumentException("Usage: <schema-file> <num-partitions> <read-max-records-per-file> <sketch-size> <output-split-points-file> <parquet-paths-as-separate-args>");
        }

        Path schemaFile = Paths.get(args[0]);
        int numPartitions = Integer.parseInt(args[1]);
        long recordsPerFile = Long.parseLong(args[2]);
        int sketchSize = Integer.parseInt(args[3]);
        Path outputFile = Paths.get(args[4]);
        List<org.apache.hadoop.fs.Path> parquetPaths = List.of(args).subList(5, args.length).stream()
                .map(org.apache.hadoop.fs.Path::new)
                .collect(toUnmodifiableList());

        String schemaJson = Files.readString(schemaFile);
        Schema schema = new SchemaSerDe().fromJson(schemaJson);
        Configuration conf = HadoopConfigurationProvider.getConfigurationForClient();
        List<Object> splitPoints = estimate(schema, conf, numPartitions, recordsPerFile, sketchSize, parquetPaths);
        try (BufferedWriter writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)) {
            writeSplitPoints(splitPoints, writer, false);
        }
    }

    public static List<Object> estimate(
            Schema schema, Configuration conf, int numPartitions, long recordsPerFile, int sketchSize,
            List<org.apache.hadoop.fs.Path> parquetPaths) throws IOException {
        try (CloseableIterator<Row> iterator = openRecordIterator(schema, conf, recordsPerFile, parquetPaths)) {
            return new EstimateSplitPoints(schema, () -> iterator, numPartitions, sketchSize).estimate();
        }
    }

    private static ConcatenatingIterator openRecordIterator(
            Schema schema, Configuration conf, long recordsPerFile, List<org.apache.hadoop.fs.Path> parquetPaths) {
        return new ConcatenatingIterator(parquetPaths.stream()
                .map(path -> recordIteratorSupplier(schema, conf, recordsPerFile, path))
                .collect(toUnmodifiableList()));
    }

    private static Supplier<CloseableIterator<Row>> recordIteratorSupplier(
            Schema schema, Configuration conf, long maxRecords, org.apache.hadoop.fs.Path dataFile) {
        return () -> {
            try {
                return new LimitingIterator<>(maxRecords,
                        new ParquetReaderIterator(new ParquetRecordReader.Builder(dataFile, schema)
                                .withConf(conf)
                                .build()));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }
}
