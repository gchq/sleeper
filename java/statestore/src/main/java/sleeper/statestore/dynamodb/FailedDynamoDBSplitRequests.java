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
import sleeper.core.statestore.SplitFileReferenceRequest;
import sleeper.core.statestore.exception.FileNotFoundException;
import sleeper.core.statestore.exception.FileReferenceAlreadyExistsException;
import sleeper.core.statestore.exception.FileReferenceAssignedToJobException;
import sleeper.core.statestore.exception.FileReferenceNotFoundException;
import sleeper.core.statestore.exception.SplitRequestsFailedException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static sleeper.dynamodb.tools.DynamoDBUtils.isConditionCheckFailure;

/**
 * Reads a DynamoDB transaction cancellation and converts it into a state store exception when splitting file
 * references.
 */
class FailedDynamoDBSplitRequests {

    private final TransactionCanceledException e;
    private final List<RequestReferenceFailures> requestReferenceFailures;
    private final Map<String, CancellationReason> referenceCountFailureByFilename;

    private FailedDynamoDBSplitRequests(
            TransactionCanceledException e,
            List<RequestReferenceFailures> requestReferenceFailures,
            Map<String, CancellationReason> referenceCountFailureByFilename) {
        this.e = e;
        this.requestReferenceFailures = requestReferenceFailures;
        this.referenceCountFailureByFilename = referenceCountFailureByFilename;
    }

    static FailedDynamoDBSplitRequests from(TransactionCanceledException e, DynamoDBSplitRequestsBatch batch) {
        List<CancellationReason> cancellationReasons = e.getCancellationReasons();
        int referenceWrites = batch.getReferenceWrites().size();
        List<CancellationReason> referenceReasons = cancellationReasons.subList(0, referenceWrites);
        List<CancellationReason> referenceCountReasons = cancellationReasons.subList(referenceWrites, cancellationReasons.size());

        return new FailedDynamoDBSplitRequests(e,
                requestReferenceFailures(batch, referenceReasons),
                referenceCountFailureByFilename(batch, referenceCountReasons));
    }

    SplitRequestsFailedException buildSplitRequestsFailedException(
            List<SplitFileReferenceRequest> successfulRequests, List<SplitFileReferenceRequest> failedRequests,
            DynamoDBFileReferenceFormat fileReferenceFormat) {
        return new SplitRequestsFailedException(successfulRequests, failedRequests,
                buildSplitRequestsFailedCause(fileReferenceFormat));
    }

    private Throwable buildSplitRequestsFailedCause(DynamoDBFileReferenceFormat fileReferenceFormat) {
        for (Map.Entry<String, CancellationReason> entry : referenceCountFailureByFilename.entrySet()) {
            if (isConditionCheckFailure(entry.getValue())) {
                return new FileNotFoundException(entry.getKey(), e);
            }
        }
        for (RequestReferenceFailures reasons : requestReferenceFailures) {
            if (isConditionCheckFailure(reasons.reasonDeleteOldFailed)) {
                Map<String, AttributeValue> item = reasons.reasonDeleteOldFailed.getItem();
                if (item == null) {
                    return new FileReferenceNotFoundException(reasons.request.getOldReference(), e);
                }
                FileReference failedDelete = fileReferenceFormat.getFileReferenceFromAttributeValues(item);
                if (failedDelete.getJobId() != null) {
                    return new FileReferenceAssignedToJobException(failedDelete, e);
                } else {
                    return new FileReferenceNotFoundException(failedDelete, e);
                }
            }
            for (int i = 0; i < reasons.reasonsAddNewFailed.size(); i++) {
                if (isConditionCheckFailure(reasons.reasonsAddNewFailed.get(i))) {
                    return new FileReferenceAlreadyExistsException(reasons.request.getNewReferences().get(i), e);
                }
            }
        }
        return e;
    }

    private static List<RequestReferenceFailures> requestReferenceFailures(
            DynamoDBSplitRequestsBatch batch, List<CancellationReason> referenceReasons) {

        int reasonsOffset = 0;
        List<RequestReferenceFailures> requestFailures = new ArrayList<>();
        for (SplitFileReferenceRequest request : batch.getRequests()) {
            CancellationReason reasonDeleteOldFailed = referenceReasons.get(reasonsOffset);
            reasonsOffset++;
            List<CancellationReason> reasonsAddNewFailed = referenceReasons.subList(
                    reasonsOffset, reasonsOffset + request.getNewReferences().size());
            reasonsOffset += request.getNewReferences().size();
            requestFailures.add(new RequestReferenceFailures(request, reasonDeleteOldFailed, reasonsAddNewFailed));
        }
        return requestFailures;
    }

    private static Map<String, CancellationReason> referenceCountFailureByFilename(
            DynamoDBSplitRequestsBatch batch, List<CancellationReason> referenceCountReasons) {
        Map<String, CancellationReason> failureByFilename = new TreeMap<>();
        int reasonsOffset = 0;
        for (String filename : batch.getReferenceCountIncrementByFilename().keySet()) {
            failureByFilename.put(filename, referenceCountReasons.get(reasonsOffset));
            reasonsOffset++;
        }
        return failureByFilename;
    }

    /**
     * Tracks failures for a single request to split a file reference.
     */
    static class RequestReferenceFailures {
        private final SplitFileReferenceRequest request;
        private final CancellationReason reasonDeleteOldFailed;
        private final List<CancellationReason> reasonsAddNewFailed;

        RequestReferenceFailures(
                SplitFileReferenceRequest request,
                CancellationReason reasonDeleteOldFailed,
                List<CancellationReason> reasonsAddNewFailed) {
            this.request = request;
            this.reasonDeleteOldFailed = reasonDeleteOldFailed;
            this.reasonsAddNewFailed = reasonsAddNewFailed;
        }
    }
}
