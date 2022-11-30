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
package sleeper.status.report.compaction.job.query;

import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.status.report.compaction.job.CompactionJobQuery;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.TimeZone;

public class RangeCompactionJobQuery implements CompactionJobQuery {

    private static final String DATE_FORMAT = "yyyyMMddhhmmss";

    private final String tableName;
    private final Instant start;
    private final Instant end;

    public RangeCompactionJobQuery(String tableName, Instant start, Instant end) {
        this.tableName = tableName;
        this.start = start;
        this.end = end;
    }

    public static RangeCompactionJobQuery fromParameters(String tableName, String queryParameters, Clock clock) {
        if (queryParameters == null) {
            Instant end = clock.instant();
            Instant start = end.minus(Duration.ofHours(4));
            return new RangeCompactionJobQuery(tableName, start, end);
        } else {
            Instant start = parseDate(queryParameters.split(",")[0]);
            Instant end = parseDate(queryParameters.split(",")[1]);
            return new RangeCompactionJobQuery(tableName, start, end);
        }
    }

    @Override
    public List<CompactionJobStatus> run(CompactionJobStatusStore statusStore) {
        return statusStore.getJobsInTimePeriod(tableName, start, end);
    }

    private static Instant parseDate(String input) {
        try {
            return createDateInputFormat().parse(input).toInstant();
        } catch (ParseException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static SimpleDateFormat createDateInputFormat() {
        SimpleDateFormat dateInputFormat = new SimpleDateFormat(DATE_FORMAT);
        dateInputFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return dateInputFormat;
    }
}
