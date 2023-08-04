/*
 * Copyright 2022-2023 Crown Copyright
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
import sleeper.core.statestore.StateStoreException;
import sleeper.ingest.IngestResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class FixedIngestJobSource implements IngestJobSource {

    private final List<IngestJob> jobs;
    private final List<IngestResult> ingestResults = new ArrayList<>();

    private FixedIngestJobSource(List<IngestJob> jobs) {
        this.jobs = Objects.requireNonNull(jobs, "jobs must not be null");
    }

    public static FixedIngestJobSource with(IngestJob... jobs) {
        return new FixedIngestJobSource(Arrays.asList(jobs));
    }

    public static FixedIngestJobSource empty() {
        return new FixedIngestJobSource(Collections.emptyList());
    }

    @Override
    public void consumeJobs(IngestJobHandler runJob) throws IteratorException, StateStoreException, IOException {
        for (IngestJob job : jobs) {
            ingestResults.add(runJob.ingest(job));
        }
    }

    public List<IngestResult> getIngestResults() {
        return Collections.unmodifiableList(ingestResults);
    }
}
