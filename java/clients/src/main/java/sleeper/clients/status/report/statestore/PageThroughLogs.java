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
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;

public class PageThroughLogs<T extends LogEntry> {

    private final long limit;
    private final Duration pagingAge;
    private final GetLogs<T> getLogs;
    private final Supplier<Instant> timeSupplier;
    private final Waiter waiter;

    public PageThroughLogs(long limit, Duration pagingAge, GetLogs<T> getLogs, Supplier<Instant> timeSupplier, Waiter waiter) {
        this.limit = limit;
        this.pagingAge = pagingAge;
        this.getLogs = getLogs;
        this.timeSupplier = timeSupplier;
        this.waiter = waiter;
    }

    public List<T> getLogsInPeriod(Instant startTime, Instant endTime) {
        List<T> logs = getLogs.getLogsInPeriod(startTime, endTime);
        if (logs.size() == limit) {
            return pageThroughRemainingLogs(startTime, endTime, logs)
                    .flatMap(List::stream)
                    .collect(toUnmodifiableList());
        } else {
            return logs;
        }
    }

    private Stream<List<T>> pageThroughRemainingLogs(Instant startTime, Instant endTime, List<T> logs) {
        Instant timeNow = timeSupplier.get();
        Instant maxPagingTime = timeNow.minus(pagingAge);
        int lastEntryIndex = logs.size() - 1;
        Instant lastEntryTime = getTruncatedTimestamp(logs.get(lastEntryIndex));
        // Apply minimum age for paging, to avoid the case where new records are ingested for a page we've already read
        if (lastEntryTime.isAfter(maxPagingTime)) {
            waiter.waitFor(Duration.between(timeNow, lastEntryTime.plus(pagingAge)));
            lastEntryIndex = findLastLogMeetingPagingAge(logs, maxPagingTime);
            if (lastEntryIndex == -1) { // No logs are old enough to retain, so refresh whole page
                return pageThroughRemainingLogs(startTime, endTime, getLogs.getLogsInPeriod(startTime, endTime));
            } else { // Avoid refreshing logs that are already old enough
                lastEntryTime = getTruncatedTimestamp(logs.get(lastEntryIndex));
            }
        }

        List<T> logsSoFar = logsBeforeLastEntry(logs, lastEntryIndex, lastEntryTime);
        List<T> remainingLogs = getLogs.getLogsInPeriod(lastEntryTime, endTime);
        if (remainingLogs.size() == limit) {
            return Stream.concat(Stream.of(logsSoFar), pageThroughRemainingLogs(lastEntryTime, endTime, remainingLogs));
        } else {
            return Stream.of(logsSoFar, remainingLogs);
        }
    }

    private List<T> logsBeforeLastEntry(List<T> logs, int lastEntryIndex, Instant lastEntryTime) {
        for (int i = lastEntryIndex - 1; i >= 0; i--) {
            Instant entryTime = logs.get(i).getTimestamp();
            if (entryTime.isBefore(lastEntryTime)) {
                return logs.subList(0, i + 1);
            }
        }
        throw new IllegalStateException("Found page with logs all at the same time, cannot split by period. All logs were at time: " + lastEntryTime);
    }

    private int findLastLogMeetingPagingAge(List<T> logs, Instant minPagingTime) {
        for (int i = logs.size() - 2; i >= 0; i--) {
            Instant entryTime = getTruncatedTimestamp(logs.get(i));
            if (entryTime.compareTo(minPagingTime) <= 0) {
                return i;
            }
        }
        return -1;
    }

    private Instant getTruncatedTimestamp(T log) {
        return log.getTimestamp().truncatedTo(ChronoUnit.SECONDS);
    }

    public interface GetLogs<T extends LogEntry> {
        List<T> getLogsInPeriod(Instant startTime, Instant endTime);
    }

    public interface Waiter {
        void waitFor(Duration duration);
    }

}
