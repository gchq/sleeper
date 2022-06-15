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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.configuration.properties.InstanceProperties;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.*;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.io.parquet.record.ParquetRecordWriter;
import sleeper.io.parquet.record.SchemaConverter;
import sleeper.query.model.Query;

/**
 * An implementation of {@link ResultsOutput} that writes results to Parquet files in an S3 bucket.
 */
public class S3ResultsOutput implements ResultsOutput {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3ResultsOutput.class);

    public static final String S3 = "S3";
    public static final String S3_BUCKET = "bucket";
    public static final String COMPRESSION_CODEC = "compressionCodec";
    public static final String ROW_GROUP_SIZE = "rowGroupSize";
    public static final String PAGE_SIZE = "pageSize";
    private final Schema schema;
    private String s3Bucket;
    private final String compressionCodec;
    private final String fileSystem;
    private final long rowGroupSize;
    private final int pageSize;

    public S3ResultsOutput(InstanceProperties instanceProperties, Schema schema, Map<String, String> config) {
        this.schema = schema;
        this.s3Bucket = config.get(S3_BUCKET);
        if (null == this.s3Bucket) {
            this.s3Bucket = instanceProperties.get(QUERY_RESULTS_BUCKET);
        }
        if (null == this.s3Bucket) {
            throw new IllegalArgumentException("Bucket to output results to cannot be found in either the config or the instance properties");
        }

        String defaultRowGroupSize = instanceProperties.get(DEFAULT_RESULTS_ROW_GROUP_SIZE);
        String defaultPageSize = instanceProperties.get(DEFAULT_RESULTS_PAGE_SIZE);

        this.compressionCodec = config.getOrDefault(COMPRESSION_CODEC, "zstd");
        this.fileSystem = instanceProperties.get(FILE_SYSTEM);
        this.rowGroupSize = Long.parseLong(config.getOrDefault(ROW_GROUP_SIZE, defaultRowGroupSize));
        this.pageSize = Integer.parseInt(config.getOrDefault(PAGE_SIZE, defaultPageSize));
    }

    @Override
    public ResultsOutputInfo publish(Query query, CloseableIterator<Record> results) {
        String outputFile = fileSystem + s3Bucket + "/query-" + query.getQueryId() + "/" + UUID.randomUUID() + ".parquet";
        ResultsOutputLocation outputLocation = new ResultsOutputLocation("s3", outputFile);

        ParquetRecordWriter.Builder builder = new ParquetRecordWriter.Builder(new Path(outputFile),
                SchemaConverter.getSchema(schema), schema)
                .withCompressionCodec(CompressionCodecName.fromConf(compressionCodec))
                .withRowGroupSize(rowGroupSize)
                .withPageSize(pageSize);
        LOGGER.info("Opening writer for results of query {} to {}", query.getQueryId(), outputFile);
        long count = 0L;
        try (ParquetWriter<Record> writer = builder.build()) {
            long startTime = System.currentTimeMillis();
            while (results.hasNext()) {
                writer.write(results.next());
                count++;
                if (0 == count % 1_000_000) {
                    LOGGER.info("Wrote {} results", count);
                }
            }
            long finishTime = System.currentTimeMillis();
            double durationInSeconds = (finishTime - startTime) / 1000.0;
            double rate = count / durationInSeconds;
            LOGGER.info("Wrote {} records to {} in {} seconds (rate of {})",
                    count, outputFile, durationInSeconds, rate);
            return new ResultsOutputInfo(count, Collections.singletonList(outputLocation));
        } catch (Exception e) {
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
}
