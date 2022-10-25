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
package sleeper.ingest;

import org.apache.datasketches.quantiles.ItemsSketch;
import org.junit.Test;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.core.iterator.IteratorException;
import sleeper.core.record.Record;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.dynamodb.DynamoDBStateStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.assertSketches;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.defaultPropertiesBuilder;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.getRecords;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.readRecordsFromParquetFile;

public class IngestRecordsFromIteratorIT extends IngestRecordsITBase {
    @Test
    public void shouldWriteRecordsCorrectly() throws StateStoreException, IOException, InterruptedException, IteratorException, ObjectFactoryException {
        // Given
        DynamoDBStateStore stateStore = getStateStore(schema);
        String localDir = folder.newFolder().getAbsolutePath();

        // When
        IngestProperties properties = defaultPropertiesBuilder(stateStore, schema,
                localDir, folder.newFolder().getAbsolutePath()).build();
        long numWritten = new IngestRecordsFromIterator(properties, getRecords().iterator()).write();

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
        assertThat(readRecords).containsExactly(getRecords().get(0), getRecords().get(1));
        //  - Local files should have been deleted
        assertThat(Files.walk(Paths.get(localDir)).filter(Files::isRegularFile).count()).isZero();
        //  - Check quantiles sketches have been written and are correct (NB the sketches are stochastic so may not be identical)
        ItemsSketch<Long> blankSketch = ItemsSketch.getInstance(1024, Comparator.naturalOrder());
        assertSketches(getRecords().stream().map(r -> (Long) r.get("key")), schema, "key", blankSketch, fileInfo);
    }


    @Test
    public void shouldWriteNoRecordsSuccessfully() throws StateStoreException, IOException, InterruptedException, IteratorException, ObjectFactoryException {
        // Given
        DynamoDBStateStore stateStore = getStateStore(schema);

        // When
        IngestProperties properties = defaultPropertiesBuilder(stateStore, schema,
                folder.newFolder().getAbsolutePath(), folder.newFolder().getAbsolutePath()).build();
        long numWritten = new IngestRecordsFromIterator(properties, Collections.emptyIterator()).write();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isZero();
        //  - Check StateStore has correct information
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        assertThat(activeFiles).isEmpty();
    }
}
