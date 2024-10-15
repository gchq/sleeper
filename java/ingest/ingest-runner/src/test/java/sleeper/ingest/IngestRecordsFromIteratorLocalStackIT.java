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
package sleeper.ingest;

import org.junit.jupiter.api.Test;

import sleeper.core.record.Record;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.sketches.testutils.SketchesDeciles;

import java.nio.file.Paths;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.getRecords;

public class IngestRecordsFromIteratorLocalStackIT extends IngestRecordsLocalStackITBase {
    @Test
    public void shouldWriteRecordsCorrectly() throws Exception {
        // Given
        StateStore stateStore = initialiseStateStore();
        List<Record> records = getRecords();

        // When
        long numWritten = ingestFromRecordIterator(stateStore, records.iterator()).getRecordsWritten();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isEqualTo(records.size());
        //  - Check StateStore has correct information
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        List<FileReference> fileReferences = stateStore.getFileReferences();
        assertThat(fileReferences)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(
                        fileReferenceFactory.rootFile(2L));
        //  - Read file and check it has correct records
        assertThat(readRecords(fileReferences.get(0)))
                .containsExactlyElementsOf(records);
        //  - Local files should have been deleted
        assertThat(Paths.get(inputFolderName)).isEmptyDirectory();
        //  - Check quantiles sketches have been written and are correct
        assertThat(SketchesDeciles.fromFile(schema, fileReferences.get(0)))
                .isEqualTo(SketchesDeciles.from(schema, records));
    }
}
