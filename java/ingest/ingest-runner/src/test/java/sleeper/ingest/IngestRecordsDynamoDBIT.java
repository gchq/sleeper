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
package sleeper.ingest;

import org.junit.jupiter.api.Test;

import sleeper.core.record.Record;
import sleeper.core.statestore.FileInfo;
import sleeper.ingest.testutils.AssertQuantiles;
import sleeper.statestore.dynamodb.DynamoDBStateStore;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.getRecords;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.getSketches;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.readRecordsFromParquetFile;

public class IngestRecordsDynamoDBIT extends IngestRecordsDynamoDBITBase {
    @Test
    public void shouldWriteRecordsCorrectly() throws Exception {
        // Given
        DynamoDBStateStore stateStore = getStateStore(schema);

        // When
        long numWritten = ingestRecords(schema, stateStore, getRecords()).getRecordsWritten();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isEqualTo(getRecords().size());
        //  - Check StateStore has correct information
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        assertThat(activeFiles).hasSize(1);
        FileInfo fileInfo = activeFiles.get(0);
        assertThat((long) fileInfo.getMinRowKey().get(0)).isOne();
        assertThat((long) fileInfo.getMaxRowKey().get(0)).isEqualTo(3L);
        assertThat(fileInfo.getNumberOfRecords().longValue()).isEqualTo(2L);
        assertThat(fileInfo.getPartitionId()).isEqualTo(stateStore.getAllPartitions().get(0).getId());
        //  - Read file and check it has correct records
        List<Record> readRecords = readRecordsFromParquetFile(fileInfo.getFilename(), schema);
        assertThat(readRecords).hasSize(2);
        assertThat(readRecords.get(0)).isEqualTo(getRecords().get(0));
        assertThat(readRecords.get(1)).isEqualTo(getRecords().get(1));
        //  - Local files should have been deleted
        assertThat(Files.walk(Paths.get(inputFolderName)).filter(Files::isRegularFile).count()).isZero();
        //  - Check quantiles sketches have been written and are correct (NB the sketches are stochastic so may not be identical)
        AssertQuantiles.forSketch(getSketches(schema, fileInfo.getFilename()).getQuantilesSketch("key"))
                .min(1L).max(3L)
                .quantile(0.0, 1L).quantile(0.1, 1L)
                .quantile(0.2, 1L).quantile(0.3, 1L)
                .quantile(0.4, 1L).quantile(0.5, 3L)
                .quantile(0.6, 3L).quantile(0.7, 3L)
                .quantile(0.8, 3L).quantile(0.9, 3L).verify();
    }

    @Test
    public void shouldWriteNoRecordsSuccessfully() throws Exception {
        // Given
        DynamoDBStateStore stateStore = getStateStore(schema);

        // When
        long numWritten = ingestRecords(schema, stateStore, Collections.emptyList()).getRecordsWritten();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isZero();
        //  - Check StateStore has correct information
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        assertThat(activeFiles).isEmpty();
    }
}
