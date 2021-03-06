/*
 * Copyright 2022 Crown Copyright
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
package sleeper.query.model.output;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.core.iterator.WrappedIterator;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.*;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.query.model.Query;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.*;
import static sleeper.query.model.output.S3ResultsOutput.PAGE_SIZE;
import static sleeper.query.model.output.S3ResultsOutput.ROW_GROUP_SIZE;

public class S3ResultsOutputTest {
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    InstanceProperties instanceProperties;
    Schema schema = new Schema();
    List<Record> recordList;
    String outputDir;

    @Before
    public void setup() throws IOException {
        setupInstanceProperties();
        setupSchema();
        setupData();
    }

    @Test
    public void testDefaultConfig() throws Exception {
        //Given
        ResultsOutput resultsOutput = new S3ResultsOutput(instanceProperties, schema, new HashMap<>());
        Query query = new Query("table", "query-id", Collections.emptyList());

        //When
        resultsOutput.publish(query, new WrappedIterator<>(recordList.iterator()));

        //Then
        String pathToResultsFile = getParquetFilesWithinDirPath(outputDir);
        int numberOfBlocks = getMetaData(pathToResultsFile).getBlocks().size();
        assertEquals("Results list matches records", recordList, getRecordsFromOutput(pathToResultsFile));
        assertEquals("There is only one block as rowGroup size is large", 1, numberOfBlocks);
    }

    @Test
    public void testPassingPageSizeAsParam() throws Exception {
        //Given
        Map<String, String> config = new HashMap<>();
        config.put(ROW_GROUP_SIZE, "1024");
        config.put(PAGE_SIZE, "1024");
        ResultsOutput resultsOutput = new S3ResultsOutput(instanceProperties, schema, config);
        Query query = new Query("table", "query-id", Collections.emptyList());

        //When
        resultsOutput.publish(query, new WrappedIterator<>(recordList.iterator()));

        //Then
        String pathToResultsFile = getParquetFilesWithinDirPath(outputDir);
        int numberOfBlocks = getMetaData(pathToResultsFile).getBlocks().size();
        assertEquals("Results list matches records", recordList, getRecordsFromOutput(pathToResultsFile));
        assertTrue("There are several blocks as rowGroup size is small", numberOfBlocks > 10);
    }

    @Test
    public void testNonDefaultPageSize() throws Exception {
        //Given
        instanceProperties.set(DEFAULT_RESULTS_ROW_GROUP_SIZE, "1024");
        instanceProperties.set(DEFAULT_RESULTS_PAGE_SIZE, "1020");
        ResultsOutput resultsOutput = new S3ResultsOutput(instanceProperties, schema, new HashMap<>());
        Query query = new Query("table", "query-id", Collections.emptyList());

        //When
        resultsOutput.publish(query, new WrappedIterator<>(recordList.iterator()));

        //Then
        String pathToResultsFile = getParquetFilesWithinDirPath(outputDir);
        int numberOfBlocks = getMetaData(pathToResultsFile).getBlocks().size();
        assertEquals("Results list matches records", recordList, getRecordsFromOutput(pathToResultsFile));
        assertTrue("There are several blocks as rowGroup size is small", numberOfBlocks > 10);
    }

    private String getParquetFilesWithinDirPath(String dir) throws IOException {
        int levelsDeep = 5; //the results are a few level deep
        try (Stream<Path> stream = Files.walk(Paths.get(dir), levelsDeep)) {
            List<String> files = stream
                    .filter(file -> !Files.isDirectory(file))
                    .map(Path::toAbsolutePath)
                    .map(Path::toString)
                    .filter(s -> s.endsWith(".parquet"))
                    .collect(Collectors.toList());

            assertEquals("one results Parquet file in dir", 1, files.size());
            return files.get(0);
        }
    }

    private ParquetMetadata getMetaData(String path) throws IOException {
        Configuration conf = new Configuration();
        org.apache.hadoop.fs.Path inputPath = new org.apache.hadoop.fs.Path(path);
        ParquetFileReader fileReader = ParquetFileReader.open(HadoopInputFile.fromPath(inputPath, conf));
        return fileReader.getFooter();
    }

    private List<Record> getRecordsFromOutput(String path) {
        List<Record> records = new ArrayList<>();
        try {
            ParquetRecordReader reader = new ParquetRecordReader(new org.apache.hadoop.fs.Path(path), schema);

            Record record = reader.read();
            while (null != record) {
                records.add(new Record(record));
                record = reader.read();
            }
            reader.close();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return records;
    }

    private void setupInstanceProperties() throws IOException {
        outputDir = tempDir.newFolder().getAbsolutePath();
        instanceProperties = new InstanceProperties();
        String queryResultsBucket = UUID.randomUUID().toString();
        instanceProperties.set(QUERY_RESULTS_BUCKET, queryResultsBucket);
        instanceProperties.set(FILE_SYSTEM, outputDir + "/");
    }

    private void setupSchema() {
        schema.setRowKeyFields(
                new Field("year", new IntType()),
                new Field("month", new IntType()),
                new Field("day", new IntType())
        );
        schema.setSortKeyFields(new Field("timestamp", new LongType()));
        schema.setValueFields(new Field("count", new LongType()),
                new Field("map", new MapType(new StringType(), new StringType())),
                new Field("str", new StringType()),
                new Field("list", new ListType(new StringType())));
    }

    private void setupData() {
        int minYear = 2010;
        int maxYear = 2020;
        LocalDate startDate = LocalDate.of(minYear, 1, 1);
        LocalDate endDate = LocalDate.of(maxYear, 12, 31);
        recordList = new ArrayList<>();
        for (LocalDate date = startDate; date.isBefore(endDate); date = date.plusDays(1)) {
            Record record = new Record();
            record.put("year", date.getYear());
            record.put("month", date.getMonthValue());
            record.put("day", date.getDayOfMonth());
            record.put("timestamp", Date.from(Timestamp.valueOf(date.atStartOfDay()).toInstant()).getTime());
            record.put("count", (long) date.getYear() * (long) date.getMonthValue() * (long) date.getDayOfMonth());
            HashMap<String, String> map = new HashMap<>();
            map.put(date.getMonth().name(), date.getMonth().name());
            record.put("map", map);
            record.put("list", Lists.newArrayList(date.getEra().toString()));
            record.put("str", date.toString());
            recordList.add(record);
        }

    }
}