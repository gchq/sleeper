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

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;

public class PagingStateStoreCommitterLogs {

    private final long limit;
    private final Duration pagingAge;
    private final GetLogs getLogs;
    private final Supplier<Instant> timeSupplier;
    private final Waiter waiter;

    public PagingStateStoreCommitterLogs(long limit, Duration pagingAge, GetLogs getLogs, Supplier<Instant> timeSupplier, Waiter waiter) {
        this.limit = limit;
        this.pagingAge = pagingAge;
        this.getLogs = getLogs;
        this.timeSupplier = timeSupplier;
        this.waiter = waiter;
    }

    public List<StateStoreCommitterLogEntry> getLogsInPeriod(Instant startTime, Instant endTime) {
        List<StateStoreCommitterLogEntry> logs = getLogs.getLogsInPeriod(startTime, endTime);
        if (logs.size() == limit) {
            timeSupplier.get();
            return pageThroughRemainingLogs(logs, endTime)
                    .flatMap(List::stream)
                    .collect(toUnmodifiableList());
        } else {
            return logs;
        }
    }

    private Stream<List<StateStoreCommitterLogEntry>> pageThroughRemainingLogs(List<StateStoreCommitterLogEntry> logs, Instant endTime) {
        Instant nextQueryStartTime = logs.get(logs.size() - 1).getTimeInCommitter();
        List<StateStoreCommitterLogEntry> logsSoFar = logsBeforeLastEntry(logs, nextQueryStartTime);
        List<StateStoreCommitterLogEntry> remainingLogs = getLogs.getLogsInPeriod(nextQueryStartTime, endTime);
        if (remainingLogs.size() == limit) {
            return Stream.concat(Stream.of(logsSoFar), pageThroughRemainingLogs(remainingLogs, endTime));
        } else {
            return Stream.of(logsSoFar, remainingLogs);
        }
    }

    private static List<StateStoreCommitterLogEntry> logsBeforeLastEntry(List<StateStoreCommitterLogEntry> logs, Instant lastEntryTime) {
        for (int i = logs.size() - 2; i >= 0; i--) {
            Instant entryTime = logs.get(i).getTimeInCommitter();
            if (entryTime.isBefore(lastEntryTime)) {
                return logs.subList(0, i + 1);
            }
        }
        throw new IllegalStateException("Found page with logs all at the same time, cannot split by period. All logs were at time: " + lastEntryTime);
    }

    public interface GetLogs {
        List<StateStoreCommitterLogEntry> getLogsInPeriod(Instant startTime, Instant endTime);
    }

    public interface Waiter {
        void waitFor(Duration duration);
    }

}
