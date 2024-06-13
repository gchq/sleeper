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
package sleeper.ingest.job;

import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.statestore.StateStoreException;
import sleeper.ingest.IngestResult;

import java.io.IOException;

/**
 * Runs an ingest job by sorting the input files, writing the files to a Sleeper table, and adding them to the
 * state store.
 */
public interface IngestJobHandler {
    /**
     * Runs an ingest job by sorting the input files, writing the files to a Sleeper table, and adding them to the
     * state store.
     *
     * @param  job                       the ingest job to run
     * @return                           an {@link IngestResult} object
     * @throws IteratorCreationException if the Sleeper table iterator could not be created
     * @throws StateStoreException       if an error occurs adding the files to the state store
     * @throws IOException               if an error occurs reading or writing files
     */
    IngestResult ingest(IngestJob job) throws IteratorCreationException, StateStoreException, IOException;
}
