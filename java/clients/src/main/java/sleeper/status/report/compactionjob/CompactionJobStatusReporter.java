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

package sleeper.status.report.compactionjob;

import sleeper.compaction.job.status.CompactionJobStatus;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Function;

public interface CompactionJobStatusReporter {

    enum QueryType {
        ALL,
        DETAILED,
        RANGE,
        UNFINISHED
    }

    void report(List<CompactionJobStatus> jobStatusList, QueryType queryType);

    static String asString(
            Function<PrintStream, CompactionJobStatusReporter> getReporter, List<CompactionJobStatus> jobStatusList, QueryType queryType)
            throws UnsupportedEncodingException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        getReporter.apply(new PrintStream(os, false, StandardCharsets.UTF_8.displayName()))
                .report(jobStatusList, queryType);
        return os.toString(StandardCharsets.UTF_8.displayName());
    }
}
