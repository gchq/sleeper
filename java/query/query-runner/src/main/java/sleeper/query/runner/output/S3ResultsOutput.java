/*
 * Copyright 2022-2024 Crown Copyright
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

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.util.LoggedDuration;
import sleeper.parquet.record.ParquetRecordWriterFactory;
import sleeper.query.model.QueryOrLeafPartitionQuery;
import sleeper.query.output.ResultsOutput;
import sleeper.query.output.ResultsOutputInfo;
import sleeper.query.output.ResultsOutputLocation;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.instance.QueryProperty.DEFAULT_RESULTS_PAGE_SIZE;
import static sleeper.core.properties.instance.QueryProperty.DEFAULT_RESULTS_ROW_GROUP_SIZE;
import static sleeper.parquet.record.ParquetRecordWriterFactory.parquetRecordWriterBuilder;

/**
 * A query results output that writes results to Parquet files in an S3 bucket.
 */
public class S3ResultsOutput implements ResultsOutput {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3ResultsOutput.class);

    public static final String S3 = "S3";
    public static final String S3_BUCKET = "bucket";
    public static final String COMPRESSION_CODEC = "compressionCodec";
    public static final String ROW_GROUP_SIZE = "rowGroupSize";
    public static final String PAGE_SIZE = "pageSize";
    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;
    private final Map<String, String> config;
    private String s3Bucket;
    private final String fileSystem;

    public S3ResultsOutput(InstanceProperties instanceProperties, TableProperties tableProperties, Map<String, String> config) {
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.config = config;
        this.s3Bucket = config.get(S3_BUCKET);
        if (null == this.s3Bucket) {
            this.s3Bucket = instanceProperties.get(QUERY_RESULTS_BUCKET);
        }
        if (null == this.s3Bucket) {
            throw new IllegalArgumentException("Bucket to output results to cannot be found in either the config or the instance properties");
        }
        this.fileSystem = instanceProperties.get(FILE_SYSTEM);
    }

    @Override
    public ResultsOutputInfo publish(QueryOrLeafPartitionQuery query, CloseableIterator<Record> results) {
        String outputFile = fileSystem + s3Bucket + "/query-" + query.getQueryId() + "/" + UUID.randomUUID() + ".parquet";
        ResultsOutputLocation outputLocation = new ResultsOutputLocation("s3", outputFile);

        LOGGER.info("Opening writer for results of query {} to {}", query.getQueryId(), outputFile);
        long count = 0L;
        try (ParquetWriter<Record> writer = buildParquetWriter(new Path(outputFile))) {
            Instant startTime = Instant.now();
            while (results.hasNext()) {
                writer.write(results.next());
                count++;
                if (0 == count % 1_000_000) {
                    LOGGER.info("Wrote {} results", count);
                }
            }
            LoggedDuration duration = LoggedDuration.withFullOutput(startTime, Instant.now());
            double rate = count / (double) duration.getSeconds();
            LOGGER.info("Wrote {} records to {} in {} (rate of {})",
                    count, outputFile, duration, rate);
            return new ResultsOutputInfo(count, Collections.singletonList(outputLocation));
        } catch (RuntimeException | IOException e) {
            LOGGER.error("Exception writing results to S3", e);
            return new ResultsOutputInfo(count, Collections.singletonList(outputLocation), e);
        } finally {
            try {
                results.close();
            } catch (IOException e) {
                LOGGER.error("IOException closing results of query", e);
            }
        }
    }

    private ParquetWriter<Record> buildParquetWriter(Path path) throws IOException {
        String defaultRowGroupSize = instanceProperties.get(DEFAULT_RESULTS_ROW_GROUP_SIZE);
        String defaultPageSize = instanceProperties.get(DEFAULT_RESULTS_PAGE_SIZE);
        ParquetRecordWriterFactory.Builder builder = parquetRecordWriterBuilder(path, tableProperties)
                .withRowGroupSize(Long.parseLong(config.getOrDefault(ROW_GROUP_SIZE, defaultRowGroupSize)))
                .withPageSize(Integer.parseInt(config.getOrDefault(PAGE_SIZE, defaultPageSize)));
        Optional.ofNullable(config.get(COMPRESSION_CODEC)).ifPresent(builder::withCompressionCodec);
        return builder.build();
    }
}
