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

import sleeper.core.statestore.FileInfoTestData;
import sleeper.ingest.IngestResult;

import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Collectors;

public class FixedIngestJobHandler {

    private FixedIngestJobHandler() {
    }

    public static IngestJobHandler makingDefaultFiles() {
        return job -> IngestResult.allReadWereWritten(job.getFiles().stream()
                .map(FileInfoTestData::defaultFileOnRootPartition)
                .collect(Collectors.toList()));
    }

    public static IngestJobHandler withResults(IngestResult... results) {
        Iterator<IngestResult> iterator = Arrays.asList(results).iterator();
        return job -> iterator.next();
    }
}
