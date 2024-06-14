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
package sleeper.commit;

import org.junit.jupiter.api.Test;

import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;

import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithSinglePartition;

public class StateStoreCommitterTest {
    private Schema schema = schemaWithKey("key");

    @Test
    void shouldApplyCompactionCommitRequest() throws Exception {
        // Given
        StateStore stateStore = inMemoryStateStoreWithSinglePartition(schema);
        FileReferenceFactory factory = FileReferenceFactory.from(stateStore);
        FileReference file1 = factory.rootFile("file1.parquet", 123L);
        stateStore.addFile(file1);
    }
}
