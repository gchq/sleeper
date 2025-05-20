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
package sleeper.compaction.job.creationv2;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.compaction.core.job.creation.CreateCompactionJobs;
import sleeper.compaction.core.job.creation.CreateCompactionJobs.GenerateBatchId;
import sleeper.compaction.core.job.creation.CreateCompactionJobs.GenerateJobId;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.core.util.ObjectFactory;
import sleeper.statestorev2.commit.SqsFifoStateStoreCommitRequestSender;

import java.time.Instant;
import java.util.Random;

public class AwsCreateCompactionJobs {

    private AwsCreateCompactionJobs() {
    }

    public static CreateCompactionJobs from(
            ObjectFactory objectFactory,
            InstanceProperties instanceProperties,
            TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider,
            S3Client s3Client,
            SqsClient sqsClient) {
        return new CreateCompactionJobs(
                objectFactory, instanceProperties, stateStoreProvider,
                new CompactionBatchJobsWriterToS3(s3Client),
                new CompactionBatchMessageSenderToSqs(instanceProperties, sqsClient),
                new SqsFifoStateStoreCommitRequestSender(instanceProperties, sqsClient, s3Client, TransactionSerDeProvider.from(tablePropertiesProvider)),
                GenerateJobId.random(), GenerateBatchId.random(), new Random(), Instant::now);
    }
}
