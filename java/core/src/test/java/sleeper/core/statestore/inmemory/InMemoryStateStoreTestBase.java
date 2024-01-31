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

package sleeper.core.statestore.inmemory;

import org.junit.jupiter.api.BeforeEach;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.SplitFileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import java.time.Duration;
import java.time.Instant;

import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithNoPartitions;

public abstract class InMemoryStateStoreTestBase {

    protected static final Instant DEFAULT_UPDATE_TIME = Instant.parse("2023-10-04T14:08:00Z");
    protected static final Instant AFTER_DEFAULT_UPDATE_TIME = DEFAULT_UPDATE_TIME.plus(Duration.ofMinutes(1));
    private final Schema schema = schemaWithKey("key", new LongType());
    private final PartitionsBuilder partitions = new PartitionsBuilder(schema).singlePartition("root");
    protected FileReferenceFactory factory = FileReferenceFactory.fromUpdatedAt(partitions.buildTree(), DEFAULT_UPDATE_TIME);
    protected final StateStore store = inMemoryStateStoreWithNoPartitions();

    @BeforeEach
    void setUp() {
        store.fixTime(DEFAULT_UPDATE_TIME);
    }


    protected void splitPartition(String parentId, String leftId, String rightId, long splitPoint) throws StateStoreException {
        partitions.splitToNewChildren(parentId, leftId, rightId, splitPoint)
                .applySplit(store, parentId);
        factory = FileReferenceFactory.fromUpdatedAt(partitions.buildTree(), DEFAULT_UPDATE_TIME);
    }

    protected FileReference splitFile(FileReference parentFile, String childPartitionId) {
        return SplitFileReference.referenceForChildPartition(parentFile, childPartitionId)
                .toBuilder().lastStateStoreUpdateTime(DEFAULT_UPDATE_TIME).build();
    }

    protected static FileReference withLastUpdate(Instant updateTime, FileReference file) {
        return file.toBuilder().lastStateStoreUpdateTime(updateTime).build();
    }
}
