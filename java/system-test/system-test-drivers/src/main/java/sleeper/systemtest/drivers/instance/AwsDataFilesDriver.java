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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.table.TableFilePaths;
import sleeper.core.util.S3Filename;
import sleeper.parquet.row.ParquetReaderIterator;
import sleeper.parquet.row.RowReadSupport;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.instance.DataFileDuplication;
import sleeper.systemtest.dsl.instance.DataFilesDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

public class AwsDataFilesDriver implements DataFilesDriver {
    public static final Logger LOGGER = LoggerFactory.getLogger(AwsDataFilesDriver.class);

    private final S3AsyncClient s3AsyncClient;
    private final Configuration hadoopConf;
    private final TableFilePaths filePaths;

    public AwsDataFilesDriver(SystemTestInstanceContext instance, SystemTestClients clients) {
        this(clients.getS3Async(), clients.createHadoopConf(),
                TableFilePaths.buildDataFilePathPrefix(instance.getInstanceProperties(), instance.getTableProperties()));
    }

    public AwsDataFilesDriver(S3AsyncClient s3AsyncClient, Configuration hadoopConf, TableFilePaths filePaths) {
        this.s3AsyncClient = s3AsyncClient;
        this.hadoopConf = hadoopConf;
        this.filePaths = filePaths;
    }

    @Override
    public CloseableIterator<Row> getRows(Schema schema, String filename) {
        try {
            LOGGER.info("Reading rows from file {}", filename);
            return new ParquetReaderIterator(
                    ParquetReader.builder(new RowReadSupport(schema), new Path(filename))
                            .withConf(hadoopConf)
                            .build());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public List<DataFileDuplication> duplicateFiles(int times, Collection<String> files) {
        LOGGER.info("Duplicating {} files", files.size());
        List<DataFileDuplication> duplications = files.stream()
                .map(file -> new DataFileDuplication(file, generateDuplicateFilePaths(times)))
                .toList();
        duplications.stream()
                .flatMap(this::performDuplication)
                .forEach(CompletableFuture::join);
        LOGGER.info("Duplicated {} files", files.size());
        return duplications;
    }

    private Stream<CompletableFuture<CopyObjectResponse>> performDuplication(DataFileDuplication duplication) {
        S3Filename original = S3Filename.parse(duplication.originalFilename());
        return duplication.newFilenames().stream()
                .map(newPath -> copy(original, S3Filename.parse(newPath)));
    }

    private CompletableFuture<CopyObjectResponse> copy(S3Filename original, S3Filename copy) {
        return s3AsyncClient.copyObject(CopyObjectRequest.builder()
                .sourceBucket(original.bucketName())
                .destinationBucket(copy.bucketName())
                .sourceKey(original.objectKey())
                .destinationKey(copy.objectKey())
                .build());
    }

    private List<String> generateDuplicateFilePaths(int numPaths) {
        List<String> paths = new ArrayList<>(numPaths);
        for (int i = 0; i < numPaths; i++) {
            paths.add(filePaths.constructPartitionParquetFilePath("duplicate", UUID.randomUUID().toString()));
        }
        return paths;
    }

}
