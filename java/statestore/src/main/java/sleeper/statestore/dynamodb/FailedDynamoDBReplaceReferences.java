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

package sleeper.statestore.dynamodb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CancellationReason;
import com.amazonaws.services.dynamodbv2.model.TransactionCanceledException;

import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.exception.FileAlreadyExistsException;
import sleeper.core.statestore.exception.FileNotFoundException;
import sleeper.core.statestore.exception.FileReferenceAlreadyExistsException;
import sleeper.core.statestore.exception.FileReferenceNotAssignedToJobException;
import sleeper.core.statestore.exception.FileReferenceNotFoundException;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static sleeper.dynamodb.tools.DynamoDBUtils.isConditionCheckFailure;

class FailedDynamoDBReplaceReferences {

    private final TransactionCanceledException e;
    private final String jobId;
    private final String partitionId;
    private final Map<String, CancellationReason> deleteOldReferenceReasonByFilename;
    private final Map<String, CancellationReason> decrementOldReferenceCountReasonByFilename;
    private final FileReference newReference;
    private final CancellationReason addNewReferenceReason;
    private final CancellationReason addNewReferenceCountReason;

    FailedDynamoDBReplaceReferences(
            TransactionCanceledException e, String jobId, String partitionId,
            Map<String, CancellationReason> deleteOldReferenceReasonByFilename,
            Map<String, CancellationReason> decrementOldReferenceCountReasonByFilename,
            FileReference newReference,
            CancellationReason addNewReferenceReason, CancellationReason addNewReferenceCountReason) {
        this.e = e;
        this.jobId = jobId;
        this.partitionId = partitionId;
        this.deleteOldReferenceReasonByFilename = deleteOldReferenceReasonByFilename;
        this.decrementOldReferenceCountReasonByFilename = decrementOldReferenceCountReasonByFilename;
        this.newReference = newReference;
        this.addNewReferenceReason = addNewReferenceReason;
        this.addNewReferenceCountReason = addNewReferenceCountReason;
    }

    static FailedDynamoDBReplaceReferences from(
            TransactionCanceledException e, String jobId, String partitionId, List<String> inputFiles, FileReference newReference) {
        List<CancellationReason> reasons = e.getCancellationReasons();
        int reasonsOffset = 0;
        List<CancellationReason> deleteOldReferenceReasons = reasons.subList(0, inputFiles.size());
        reasonsOffset += inputFiles.size();
        CancellationReason addNewReferenceReason = reasons.get(reasonsOffset);
        reasonsOffset++;
        List<CancellationReason> decrementOldReferenceCountReasons = reasons.subList(reasonsOffset, reasonsOffset + inputFiles.size());
        reasonsOffset += inputFiles.size();
        CancellationReason addNewReferenceCountReason = reasons.get(reasonsOffset);

        return new FailedDynamoDBReplaceReferences(e, jobId, partitionId,
                inputFileReasonByFilename(inputFiles, deleteOldReferenceReasons),
                inputFileReasonByFilename(inputFiles, decrementOldReferenceCountReasons),
                newReference, addNewReferenceReason, addNewReferenceCountReason);
    }

    StateStoreException buildStateStoreException(DynamoDBFileReferenceFormat fileReferenceFormat) {
        for (String filename : deleteOldReferenceReasonByFilename.keySet()) {
            if (isConditionCheckFailure(decrementOldReferenceCountReasonByFilename.get(filename))) {
                return new FileNotFoundException(filename, e);
            }
            CancellationReason deleteReferenceReason = deleteOldReferenceReasonByFilename.get(filename);
            if (isConditionCheckFailure(deleteReferenceReason)) {
                Map<String, AttributeValue> item = deleteReferenceReason.getItem();
                if (item == null) {
                    return new FileReferenceNotFoundException(filename, partitionId, e);
                }
                FileReference failedUpdate = fileReferenceFormat.getFileReferenceFromAttributeValues(item);
                if (!jobId.equals(failedUpdate.getJobId())) {
                    return new FileReferenceNotAssignedToJobException(failedUpdate, jobId, e);
                } else {
                    return new FileReferenceNotFoundException(failedUpdate, e);
                }
            }
        }
        if (isConditionCheckFailure(addNewReferenceCountReason)) {
            return new FileAlreadyExistsException(newReference.getFilename(), e);
        }
        if (isConditionCheckFailure(addNewReferenceReason)) {
            return new FileReferenceAlreadyExistsException(newReference, e);
        }
        return new StateStoreException("Failed to mark files ready for GC and add new files", e);
    }

    private static Map<String, CancellationReason> inputFileReasonByFilename(List<String> inputFiles, List<CancellationReason> reasons) {
        Map<String, CancellationReason> reasonByFilename = new TreeMap<>();
        for (int i = 0; i < reasons.size(); i++) {
            reasonByFilename.put(inputFiles.get(i), reasons.get(i));
        }
        return reasonByFilename;
    }
}
