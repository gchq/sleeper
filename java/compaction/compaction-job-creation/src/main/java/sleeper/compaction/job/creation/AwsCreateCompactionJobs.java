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
import com.amazonaws.services.sqs.AmazonSQS;

import sleeper.compaction.core.job.CompactionJobStatusStore;
import sleeper.compaction.core.job.creation.CreateCompactionJobs;
import sleeper.compaction.core.job.creation.CreateCompactionJobs.GenerateBatchId;
import sleeper.compaction.core.job.creation.CreateCompactionJobs.GenerateJobId;
import sleeper.compaction.core.job.creation.CreateCompactionJobs.Mode;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.util.ObjectFactory;

import java.time.Instant;
import java.util.Random;

public class AwsCreateCompactionJobs {

    private AwsCreateCompactionJobs() {
    }

    public static CreateCompactionJobs from(
            ObjectFactory objectFactory,
            InstanceProperties instanceProperties,
            StateStoreProvider stateStoreProvider,
            CompactionJobStatusStore jobStatusStore,
            AmazonS3 s3Client,
            AmazonSQS sqsClient,
            Mode mode) {
        return new CreateCompactionJobs(
                objectFactory, instanceProperties, stateStoreProvider,
                new SendCompactionJobToSqs(instanceProperties, sqsClient), null, null, jobStatusStore,
                new SendAssignJobIdToSqs(sqsClient, instanceProperties),
                GenerateJobId.random(), GenerateBatchId.random(), new Random(), Instant::now);
    }
}
