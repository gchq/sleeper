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

import sleeper.compaction.job.commit.CompactionJobIdAssignmentCommitRequest;
import sleeper.compaction.job.commit.CompactionJobIdAssignmentCommitRequestSerDe;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.statestore.AssignJobIdRequest;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import java.util.List;
import java.util.UUID;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;

public interface AssignJobIdToFiles {
    void assignJobIds(List<AssignJobIdRequest> requests, String tableId) throws StateStoreException;

    static AssignJobIdToFiles synchronous(StateStore stateStore) {
        return (assignJobIdRequests, tableId) -> stateStore.assignJobIds(assignJobIdRequests);
    }

    static AssignJobIdToFiles bySqs(
            AmazonSQS sqsClient, InstanceProperties instanceProperties) {
        CompactionJobIdAssignmentCommitRequestSerDe serDe = new CompactionJobIdAssignmentCommitRequestSerDe();
        return (assignJobIdRequests, tableId) -> {
            CompactionJobIdAssignmentCommitRequest request = new CompactionJobIdAssignmentCommitRequest(assignJobIdRequests, tableId);
            sqsClient.sendMessage(new SendMessageRequest()
                    .withQueueUrl(instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL))
                    .withMessageBody(serDe.toJson(request))
                    .withMessageGroupId(tableId)
                    .withMessageDeduplicationId(UUID.randomUUID().toString()));
        };
    }
}
