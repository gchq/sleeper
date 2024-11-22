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
package sleeper.compaction.job.creation.commit;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.core.job.CompactionJobStatusStore;
import sleeper.compaction.core.job.commit.CompactionJobIdAssignmentCommitRequest;
import sleeper.compaction.core.job.commit.CompactionJobIdAssignmentCommitRequestSerDe;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.statestore.AssignJobIdRequest;
import sleeper.core.statestore.StateStore;
import sleeper.core.table.TableStatus;

import java.util.List;
import java.util.UUID;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;

public interface AssignJobIdToFiles {
    Logger LOGGER = LoggerFactory.getLogger(AssignJobIdToFiles.class);

    void assignJobIds(List<AssignJobIdRequest> requests, TableStatus tableStatus);

    static AssignJobIdToFiles synchronous(StateStore stateStore, CompactionJobStatusStore statusStore) {
        return (assignJobIdRequests, tableStatus) -> {
            stateStore.assignJobIds(assignJobIdRequests);
            statusStore.jobInputFilesAssigned(tableStatus.getTableUniqueId(), assignJobIdRequests);
        };
    }

    static AssignJobIdToFiles byQueue(AssignJobIdQueueSender queueSender) {
        return (assignJobIdRequests, tableStatus) -> queueSender.send(
                new CompactionJobIdAssignmentCommitRequest(assignJobIdRequests, tableStatus.getTableUniqueId()));
    }

    interface AssignJobIdQueueSender {
        void send(CompactionJobIdAssignmentCommitRequest commitRequest);

        static AssignJobIdQueueSender bySqs(AmazonSQS sqsClient, InstanceProperties instanceProperties) {
            CompactionJobIdAssignmentCommitRequestSerDe serDe = new CompactionJobIdAssignmentCommitRequestSerDe();
            return (request) -> {
                LOGGER.debug("Sending asynchronous request to state store committer: {}", request);
                sqsClient.sendMessage(new SendMessageRequest()
                        .withQueueUrl(instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL))
                        .withMessageBody(serDe.toJson(request))
                        .withMessageGroupId(request.getTableId())
                        .withMessageDeduplicationId(UUID.randomUUID().toString()));
                LOGGER.debug("Submitted asynchronous request to assign compaction input files via state store committer queue");
            };
        }
    }

}
