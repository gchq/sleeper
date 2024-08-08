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
package sleeper.splitter;

import org.apache.hadoop.conf.Configuration;

import sleeper.core.partition.Partition;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.splitter.SplitMultiDimensionalPartitionImpl.SketchesLoader;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import static sleeper.splitter.SplitMultiDimensionalPartitionImpl.loadSketchesFromFile;

/**
 * Splits a partition. Delegates to {@link SplitMultiDimensionalPartitionImpl}.
 */
public class SplitPartition {
    private final StateStore stateStore;
    private final Schema schema;
    private final SketchesLoader sketchesLoader;
    private final Supplier<String> idSupplier;

    public SplitPartition(StateStore stateStore,
            Schema schema,
            Configuration conf) {
        this(stateStore, schema, loadSketchesFromFile(schema, conf));
    }

    public SplitPartition(StateStore stateStore,
            Schema schema,
            SketchesLoader sketchesLoader) {
        this(stateStore, schema, sketchesLoader, () -> UUID.randomUUID().toString());
    }

    public SplitPartition(StateStore stateStore,
            Schema schema,
            SketchesLoader sketchesLoader,
            Supplier<String> idSupplier) {
        this.stateStore = stateStore;
        this.schema = schema;
        this.sketchesLoader = sketchesLoader;
        this.idSupplier = idSupplier;
    }

    public void splitPartition(Partition partition, List<String> fileNames) throws StateStoreException, IOException {
        new SplitMultiDimensionalPartitionImpl(stateStore, schema, partition, fileNames, idSupplier, sketchesLoader)
                .splitPartition();
    }
}
