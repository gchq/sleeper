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
package sleeper.ingest.runner;

import org.junit.jupiter.api.Test;

import sleeper.core.row.Row;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.sketches.testutils.SketchesDeciles;

import java.nio.file.Paths;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.runner.testutils.IngestRowsTestDataHelper.getRows;

public class IngestRowsFromIteratorLocalStackIT extends IngestRowsLocalStackITBase {
    @Test
    public void shouldWriteRowsCorrectly() throws Exception {
        // Given
        StateStore stateStore = initialiseStateStore();
        List<Row> rows = getRows();

        // When
        long numWritten = ingestFromRowIterator(stateStore, rows.iterator()).getRowsWritten();

        // Then:
        //  - Check the correct number of rows were written
        assertThat(numWritten).isEqualTo(rows.size());
        //  - Check StateStore has correct information
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        List<FileReference> fileReferences = stateStore.getFileReferences();
        assertThat(fileReferences)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(
                        fileReferenceFactory.rootFile(2L));
        //  - Read file and check it has correct rows
        assertThat(readRows(fileReferences))
                .containsExactlyElementsOf(rows);
        //  - Local files should have been deleted
        assertThat(Paths.get(ingestLocalFiles)).isEmptyDirectory();
        //  - Check quantiles sketches have been written and are correct
        assertThat(SketchesDeciles.fromFile(schema, fileReferences.get(0), sketchesStore))
                .isEqualTo(SketchesDeciles.from(schema, rows));
    }
}
