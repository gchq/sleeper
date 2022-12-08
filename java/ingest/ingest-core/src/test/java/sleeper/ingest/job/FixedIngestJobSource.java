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
package sleeper.ingest.job;

import sleeper.core.iterator.IteratorException;
import sleeper.ingest.IngestResult;
import sleeper.statestore.StateStoreException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class FixedIngestJobSource implements IngestJobSource {

    private final List<IngestJob> jobs;
    private final List<IngestResult> ingestResults = new ArrayList<>();

    public FixedIngestJobSource(IngestJob... jobs) {
        this.jobs = Arrays.asList(jobs);
    }

    @Override
    public void consumeJobs(Callback runJob) throws IteratorException, StateStoreException, IOException {
        for (IngestJob job : jobs) {
            ingestResults.add(runJob.ingest(job));
        }
    }

    public List<IngestResult> getIngestResults() {
        return Collections.unmodifiableList(ingestResults);
    }
}
