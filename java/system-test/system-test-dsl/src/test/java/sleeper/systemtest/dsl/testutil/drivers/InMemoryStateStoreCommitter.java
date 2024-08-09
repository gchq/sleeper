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
package sleeper.systemtest.dsl.testutil.drivers;

import sleeper.commit.StateStoreCommitter;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStoreException;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.statestore.StateStoreCommitterDriver;

import java.time.Instant;
import java.util.stream.Stream;

public class InMemoryStateStoreCommitter {

    private final InMemoryIngestByQueue ingest;
    private final InMemoryCompaction compaction;

    public InMemoryStateStoreCommitter(InMemoryIngestByQueue ingest, InMemoryCompaction compaction) {
        this.ingest = ingest;
        this.compaction = compaction;
    }

    public StateStoreCommitterDriver withContext(SystemTestContext context) {
        return new Driver(context);
    }

    public class Driver implements StateStoreCommitterDriver {
        private final StateStoreCommitter committer;

        private Driver(SystemTestContext context) {
            SystemTestInstanceContext instance = context.instance();
            TablePropertiesProvider tablePropertiesProvider = instance.getTablePropertiesProvider();
            committer = new StateStoreCommitter(tablePropertiesProvider,
                    compaction.jobStore(), ingest.jobStore(),
                    instance.getStateStoreProvider().byTableId(tablePropertiesProvider),
                    InMemoryStateStoreCommitter::failToLoadS3Object, Instant::now);
        }

        @Override
        public void sendCommitMessages(Stream<String> messages) {
            messages.forEach(message -> {
                try {
                    committer.applyFromJson(message);
                } catch (StateStoreException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private static String failToLoadS3Object(String key) {
        throw new UnsupportedOperationException();
    }

}
