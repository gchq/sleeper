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
package sleeper.ingest.batcher.job.creator;

import com.amazonaws.services.sqs.AmazonSQS;

import sleeper.ingest.batcher.core.IngestBatcherQueueClient;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.core.job.IngestJobSerDe;

public class SQSIngestBatcherQueueClient implements IngestBatcherQueueClient {

    private final AmazonSQS sqs;
    private final IngestJobSerDe serDe = new IngestJobSerDe();

    public SQSIngestBatcherQueueClient(AmazonSQS sqs) {
        this.sqs = sqs;
    }

    @Override
    public void send(String queueUrl, IngestJob job) {
        sqs.sendMessage(queueUrl, serDe.toJson(job));
    }
}
