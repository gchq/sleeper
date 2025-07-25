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
package sleeper.compaction.job.creation;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobSerDe;
import sleeper.compaction.core.job.creation.CreateCompactionJobs;

import java.util.List;

public class CompactionBatchJobsWriterToS3 implements CreateCompactionJobs.BatchJobsWriter {

    private final S3Client s3Client;
    private final CompactionJobSerDe serDe = new CompactionJobSerDe();

    public CompactionBatchJobsWriterToS3(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    @Override
    public void writeJobs(String bucketName, String key, List<CompactionJob> compactionJobs) {
        s3Client.putObject(PutObjectRequest.builder()
                .bucket(bucketName).key(key).build(), RequestBody.fromString(serDe.toJson(compactionJobs)));
    }

}
