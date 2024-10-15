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

import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.sketches.testutils.SketchesDeciles;

import java.nio.file.Paths;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.getRecords;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.getSketches;

public class IngestRecordsFromIteratorLocalStackIT extends IngestRecordsLocalStackITBase {
    @Test
    public void shouldWriteRecordsCorrectly() throws Exception {
        // Given
        StateStore stateStore = initialiseStateStore();

        // When
        long numWritten = ingestFromRecordIterator(stateStore, getRecords().iterator()).getRecordsWritten();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isEqualTo(getRecords().size());
        //  - Check StateStore has correct information
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        List<FileReference> fileReferences = stateStore.getFileReferences();
        assertThat(fileReferences)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(
                        fileReferenceFactory.rootFile(2L));
        //  - Read file and check it has correct records
        assertThat(readRecords(fileReferences.get(0)))
                .containsExactlyElementsOf(getRecords());
        //  - Local files should have been deleted
        assertThat(Paths.get(inputFolderName)).isEmptyDirectory();
        //  - Check quantiles sketches have been written and are correct
        assertThat(SketchesDeciles.from(getSketches(schema, fileReferences.get(0).getFilename())))
                .isEqualTo(SketchesDeciles.builder()
                        .field("key", deciles -> deciles
                                .min(1L).max(3L)
                                .rank(0.1, 1L).rank(0.2, 1L).rank(0.3, 1L)
                                .rank(0.4, 1L).rank(0.5, 1L).rank(0.6, 3L)
                                .rank(0.7, 3L).rank(0.8, 3L).rank(0.9, 3L))
                        .build());
    }
}
