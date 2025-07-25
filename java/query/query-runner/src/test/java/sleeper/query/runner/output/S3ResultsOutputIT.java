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
package sleeper.query.runner.output;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.iterator.WrappedIterator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.StringType;
import sleeper.parquet.row.ParquetRowReader;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QueryOrLeafPartitionQuery;
import sleeper.query.core.output.ResultsOutput;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.instance.QueryProperty.DEFAULT_RESULTS_PAGE_SIZE;
import static sleeper.core.properties.instance.QueryProperty.DEFAULT_RESULTS_ROW_GROUP_SIZE;
import static sleeper.query.runner.output.S3ResultsOutput.PAGE_SIZE;
import static sleeper.query.runner.output.S3ResultsOutput.ROW_GROUP_SIZE;

class S3ResultsOutputIT {
    @TempDir
    public Path tempDir;

    InstanceProperties instanceProperties = new InstanceProperties();
    TableProperties tableProperties = new TableProperties(instanceProperties);

    Schema schema = setupSchema();
    List<Row> rowList = setupData();
    String outputDir;
    Query query = Query.builder()
            .tableName("table")
            .queryId("query-id")
            .regions(List.of())
            .build();

    @BeforeEach
    public void setup() throws IOException {
        outputDir = createTempDirectory(tempDir, null).toString();
        instanceProperties.set(QUERY_RESULTS_BUCKET, UUID.randomUUID().toString());
        instanceProperties.set(FILE_SYSTEM, outputDir + "/");
        tableProperties.setSchema(schema);
    }

    @Test
    void testDefaultConfig() throws Exception {
        // Given
        ResultsOutput resultsOutput = new S3ResultsOutput(instanceProperties, tableProperties, new HashMap<>());

        // When
        resultsOutput.publish(new QueryOrLeafPartitionQuery(query), new WrappedIterator<>(rowList.iterator()));

        // Then
        String pathToResultsFile = getParquetFilesWithinDirPath(outputDir);
        int numberOfBlocks = getMetaData(pathToResultsFile).getBlocks().size();
        assertThat(getRecordsFromOutput(pathToResultsFile)).as("Results list matches records").isEqualTo(rowList);
        assertThat(numberOfBlocks).as("There is only one block as rowGroup size is large").isOne();
    }

    @Test
    void testPassingPageSizeAsParam() throws Exception {
        // Given
        Map<String, String> config = new HashMap<>();
        config.put(ROW_GROUP_SIZE, "1024");
        config.put(PAGE_SIZE, "1024");
        ResultsOutput resultsOutput = new S3ResultsOutput(instanceProperties, tableProperties, config);

        // When
        resultsOutput.publish(new QueryOrLeafPartitionQuery(query), new WrappedIterator<>(rowList.iterator()));

        // Then
        String pathToResultsFile = getParquetFilesWithinDirPath(outputDir);
        int numberOfBlocks = getMetaData(pathToResultsFile).getBlocks().size();
        assertThat(getRecordsFromOutput(pathToResultsFile)).as("Results list matches records").isEqualTo(rowList);
        assertThat(numberOfBlocks).as("There are several blocks as rowGroup size is small").isGreaterThan(10);
    }

    @Test
    void testNonDefaultPageSize() throws Exception {
        // Given
        instanceProperties.set(DEFAULT_RESULTS_ROW_GROUP_SIZE, "1024");
        instanceProperties.set(DEFAULT_RESULTS_PAGE_SIZE, "1020");
        ResultsOutput resultsOutput = new S3ResultsOutput(instanceProperties, tableProperties, new HashMap<>());

        // When
        resultsOutput.publish(new QueryOrLeafPartitionQuery(query), new WrappedIterator<>(rowList.iterator()));

        // Then
        String pathToResultsFile = getParquetFilesWithinDirPath(outputDir);
        int numberOfBlocks = getMetaData(pathToResultsFile).getBlocks().size();
        assertThat(getRecordsFromOutput(pathToResultsFile)).as("Results list matches records").isEqualTo(rowList);
        assertThat(numberOfBlocks).as("There are several blocks as rowGroup size is small").isGreaterThan(10);
    }

    private String getParquetFilesWithinDirPath(String dir) throws IOException {
        int levelsDeep = 5; // the results are a few levels deep
        try (Stream<Path> stream = Files.walk(Paths.get(dir), levelsDeep)) {
            List<String> files = stream
                    .filter(file -> !Files.isDirectory(file))
                    .map(Path::toAbsolutePath)
                    .map(Path::toString)
                    .filter(s -> s.endsWith(".parquet"))
                    .collect(Collectors.toList());

            assertThat(files).as("one results Parquet file in dir").hasSize(1);
            return files.get(0);
        }
    }

    private ParquetMetadata getMetaData(String path) throws IOException {
        Configuration conf = new Configuration();
        org.apache.hadoop.fs.Path inputPath = new org.apache.hadoop.fs.Path(path);
        try (ParquetFileReader fileReader = ParquetFileReader.open(HadoopInputFile.fromPath(inputPath, conf))) {
            return fileReader.getFooter();
        }
    }

    private List<Row> getRecordsFromOutput(String path) {
        List<Row> rows = new ArrayList<>();
        try {
            ParquetRowReader reader = new ParquetRowReader(path, schema);

            Row row = reader.read();
            while (null != row) {
                rows.add(new Row(row));
                row = reader.read();
            }
            reader.close();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return rows;
    }

    private static Schema setupSchema() {
        return Schema.builder()
                .rowKeyFields(
                        new Field("year", new IntType()),
                        new Field("month", new IntType()),
                        new Field("day", new IntType()))
                .sortKeyFields(
                        new Field("timestamp", new LongType()))
                .valueFields(
                        new Field("count", new LongType()),
                        new Field("map", new MapType(new StringType(), new StringType())),
                        new Field("str", new StringType()),
                        new Field("list", new ListType(new StringType())))
                .build();
    }

    private static List<Row> setupData() {
        int minYear = 2010;
        int maxYear = 2020;
        LocalDate startDate = LocalDate.of(minYear, 1, 1);
        LocalDate endDate = LocalDate.of(maxYear, 12, 31);
        List<Row> rowList = new ArrayList<>();
        for (LocalDate date = startDate; date.isBefore(endDate); date = date.plusDays(1)) {
            Row row = new Row();
            row.put("year", date.getYear());
            row.put("month", date.getMonthValue());
            row.put("day", date.getDayOfMonth());
            row.put("timestamp", Date.from(Timestamp.valueOf(date.atStartOfDay()).toInstant()).getTime());
            row.put("count", (long) date.getYear() * (long) date.getMonthValue() * (long) date.getDayOfMonth());
            HashMap<String, String> map = new HashMap<>();
            map.put(date.getMonth().name(), date.getMonth().name());
            row.put("map", map);
            row.put("list", Lists.newArrayList(date.getEra().toString()));
            row.put("str", date.toString());
            rowList.add(row);
        }
        return rowList;
    }
}
