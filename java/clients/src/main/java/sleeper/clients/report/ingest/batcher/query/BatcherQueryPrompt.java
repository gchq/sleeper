/*
 * Copyright 2022-2025 Crown Copyright
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

package sleeper.clients.report.ingest.batcher.query;

import sleeper.clients.report.ingest.batcher.BatcherQuery;
import sleeper.clients.util.console.ConsoleInput;

public class BatcherQueryPrompt {
    private BatcherQueryPrompt() {
    }

    public static BatcherQuery from(ConsoleInput in) {
        String type = in.promptLine("All (a) or Pending (p) query? ");
        if ("a".equalsIgnoreCase(type)) {
            return new AllFilesQuery();
        } else if ("p".equalsIgnoreCase(type)) {
            return new PendingFilesQuery();
        } else {
            return from(in);
        }
    }
}
