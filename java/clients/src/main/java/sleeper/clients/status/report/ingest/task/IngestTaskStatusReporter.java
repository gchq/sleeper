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

package sleeper.clients.status.report.ingest.task;

import sleeper.ingest.core.task.IngestTaskStatus;

import java.io.PrintStream;
import java.util.List;
import java.util.Locale;

public interface IngestTaskStatusReporter {
    void report(IngestTaskQuery query, List<IngestTaskStatus> tasks);

    static IngestTaskStatusReporter from(String type, PrintStream out) {
        switch (type.toLowerCase(Locale.ROOT)) {
            case "standard":
                return new StandardIngestTaskStatusReporter(out);
            case "json":
                return new JsonIngestTaskStatusReporter(out);
            default:
                throw new IllegalArgumentException("Unrecognised reporter type: " + type);
        }
    }
}
