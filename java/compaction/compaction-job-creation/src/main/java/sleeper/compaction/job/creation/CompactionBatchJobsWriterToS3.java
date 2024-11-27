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
package sleeper.compaction.job.creation;

import com.amazonaws.services.s3.AmazonS3;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobSerDe;
import sleeper.compaction.core.job.creation.CreateCompactionJobs;

import java.util.List;

public class CompactionBatchJobsWriterToS3 implements CreateCompactionJobs.BatchJobsWriter {

    private final AmazonS3 s3Client;
    private final CompactionJobSerDe serDe = new CompactionJobSerDe();

    public CompactionBatchJobsWriterToS3(AmazonS3 s3Client) {
        this.s3Client = s3Client;
    }

    @Override
    public void writeJobs(String bucketName, String key, List<CompactionJob> compactionJobs) {
        s3Client.putObject(bucketName, key, serDe.toJson(compactionJobs));
    }

}
