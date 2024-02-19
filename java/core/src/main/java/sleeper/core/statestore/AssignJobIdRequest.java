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

package sleeper.core.statestore;

import java.util.List;

public class AssignJobIdRequest {
    private final String jobId;
    private final String partitionId;
    private final List<String> filenames;

    private AssignJobIdRequest(String jobId, String partitionId, List<String> filenames) {
        this.jobId = jobId;
        this.partitionId = partitionId;
        this.filenames = filenames;
    }

    public static AssignJobIdRequest assignJobIdRequest(String jobId, String partitionId, List<String> filenames) {
        return new AssignJobIdRequest(jobId, partitionId, filenames);
    }

    public String getJobId() {
        return jobId;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public List<String> getFilenames() {
        return filenames;
    }
}
