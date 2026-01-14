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
 * A log entry retrieved from Amazon CloudWatch.
 */
public interface LogEntry {

    /**
     * Gets the name of the log stream this entry was written to.
     *
     * @return the log stream
     */
    String getLogStream();

    /**
     * Gets the timestamp of this log entry.
     *
     * @return the timestamp
     */
    Instant getTimestamp();
}
