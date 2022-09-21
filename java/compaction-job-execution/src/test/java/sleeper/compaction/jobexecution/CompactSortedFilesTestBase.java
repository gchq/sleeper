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
package sleeper.compaction.jobexecution;

import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import sleeper.compaction.job.CompactionFactory;
import sleeper.core.CommonTestConstants;
import sleeper.core.schema.Schema;
import sleeper.statestore.DelegatingStateStore;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.inmemory.FixedPartitionStore;
import sleeper.statestore.inmemory.InMemoryFileInfoStore;

public class CompactSortedFilesTestBase {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);
    protected String folderName;

    @Before
    public void setUp() throws Exception {
        folderName = folder.newFolder().getAbsolutePath();
    }

    protected CompactionFactory compactionFactory() {
        return compactionFactoryBuilder().build();
    }

    protected CompactionFactory.Builder compactionFactoryBuilder() {
        return CompactionFactory.withTableName("table").outputFilePrefix(folderName);
    }

    protected StateStore createInitStateStore(Schema schema) throws StateStoreException {
        StateStore stateStore = createStateStore(schema);
        stateStore.initialise();
        return stateStore;
    }

    protected StateStore createStateStore(Schema schema) {
        return new DelegatingStateStore(new InMemoryFileInfoStore(), new FixedPartitionStore(schema));
    }
}
