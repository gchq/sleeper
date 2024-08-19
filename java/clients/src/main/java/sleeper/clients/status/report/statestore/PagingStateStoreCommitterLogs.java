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
package sleeper.clients.status.report.statestore;

import java.time.Instant;
import java.util.List;

public class PagingStateStoreCommitterLogs {

    private final long limit;
    private final GetLogs getLogs;

    private PagingStateStoreCommitterLogs(long limit, GetLogs getLogs) {
        this.getLogs = getLogs;
        this.limit = limit;
    }

    public List<StateStoreCommitterLogEntry> getLogsInPeriod(Instant startTime, Instant endTime) {
        List<StateStoreCommitterLogEntry> logs = getLogs.getLogsInPeriod(startTime, endTime);
        return logs;
    }

    public static PagingStateStoreCommitterLogs pageAfterLimit(long limit, GetLogs getLogs) {
        return new PagingStateStoreCommitterLogs(limit, getLogs);
    }

    public interface GetLogs {
        List<StateStoreCommitterLogEntry> getLogsInPeriod(Instant startTime, Instant endTime);
    }

}
