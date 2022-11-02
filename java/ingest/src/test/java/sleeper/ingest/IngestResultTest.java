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

import org.junit.Test;
import sleeper.core.iterator.IteratorException;
import sleeper.core.record.Record;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.getRecords;

public class IngestResultTest extends IngestRecordsTestBase {
    @Test
    public void shouldReturnNumberOfRecordsFromIngestResult() throws StateStoreException, IteratorException, IOException {
        // Given
        StateStore stateStore = getStateStore(schema);
        IngestRecords ingestRecords = new IngestRecords(defaultPropertiesBuilder(stateStore, schema).build());

        // When
        for (Record record : getRecords()) {
            ingestRecords.write(record);
        }

        // Then
        IngestResult result = ingestRecords.close();
        assertThat(result.getNumberOfRecords())
                .isEqualTo(2L);
    }

    @Test
    public void shouldReturnFileInfoListFromIngestResult() throws StateStoreException, IteratorException, IOException {
        // Given
        StateStore stateStore = getStateStore(schema);
        IngestRecords ingestRecords = new IngestRecords(defaultPropertiesBuilder(stateStore, schema).build());

        // When
        for (Record record : getRecords()) {
            ingestRecords.write(record);
        }

        // Then
        IngestResult result = ingestRecords.close();
        assertThat(result.getFileInfoList())
                .containsExactlyInAnyOrderElementsOf(stateStore.getActiveFiles());
    }
}
