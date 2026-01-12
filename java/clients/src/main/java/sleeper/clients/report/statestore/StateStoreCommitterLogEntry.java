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
package sleeper.clients.report.statestore;

import java.time.Instant;

/**
 * An entry in the logs of the state store committer. This is parsed from a log line queried from Amazon CloudWatch.
 */
public interface StateStoreCommitterLogEntry extends LogEntry {

    /**
     * Retrieves the time of the log entry as it was recorded in the state store committer.
     *
     * @return the time recorded in the committer
     */
    Instant getTimeInCommitter();
}
