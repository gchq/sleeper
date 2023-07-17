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
package sleeper.systemtest.drivers.compaction;

import com.google.gson.Gson;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import sleeper.clients.util.GsonConfig;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.core.record.process.status.ProcessRun;

import java.time.Duration;
import java.time.Instant;

@SuppressFBWarnings("URF_UNREAD_FIELD") // Fields are read by GSON
public class CompactionJobStatusJson {

    private static final Gson GSON = GsonConfig.standardBuilder().create();

    private final String jobId;
    private final Instant createTime;
    private final Instant lastStartTime;
    private final Duration durationSoFar;

    public CompactionJobStatusJson(CompactionJobStatus status) {
        jobId = status.getJobId();
        createTime = status.getCreateUpdateTime();
        lastStartTime = status.getLatestRun().map(ProcessRun::getStartTime).orElse(null);
        if (lastStartTime != null) {
            durationSoFar = Duration.between(lastStartTime, Instant.now());
        } else {
            durationSoFar = null;
        }
    }

    public String toString() {
        return GSON.toJson(this);
    }

}
