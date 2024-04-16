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

import com.amazonaws.services.dynamodbv2.model.TransactWriteItem;

import sleeper.core.statestore.SplitFileReferenceRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class DynamoDBSplitRequestsBatch {
    private final List<SplitFileReferenceRequest> requests = new ArrayList<>();
    private final List<TransactWriteItem> referenceWrites = new ArrayList<>();
    private final Map<String, Integer> referenceCountIncrementByFilename = new TreeMap<>();

    public void addRequest(SplitFileReferenceRequest request, List<TransactWriteItem> requestReferenceWrites) {
        referenceWrites.addAll(requestReferenceWrites);
        requests.add(request);

        // If the same physical file has multiple references that are each split by a different request,
        // we need to track the reference count and aggregate into one update for each file. This is because
        // DynamoDB does not allow updating the same item twice in one transaction, and we need to update the
        // reference count item for each physical file.
        int referenceCountDiff = request.getNewReferences().size() - 1;
        int referenceCountIncrement = referenceCountIncrementByFilename.getOrDefault(request.getFilename(), 0);
        referenceCountIncrementByFilename.put(request.getFilename(), referenceCountIncrement + referenceCountDiff);
    }

    public static boolean wouldOverflowOneTransaction(List<TransactWriteItem> requestReferenceWrites) {
        // DynamoDB only allows 100 TransactWriteItems in a transaction
        // Reference count update would take up another TransactWriteItem
        return requestReferenceWrites.size() > 99;
    }

    public boolean wouldOverflow(SplitFileReferenceRequest request, List<TransactWriteItem> requestReferenceWrites) {
        int newBatchWrites = getNumberOfWriteItems() + requestReferenceWrites.size();
        if (!isFileUpdated(request.getFilename())) {
            // Reference count updates need to be aggregated into one for each file, so this will only result in
            // a separate write item if this file has not been updated by a previous request
            newBatchWrites += 1;
        }
        // DynamoDB only allows 100 TransactWriteItems in a transaction
        return newBatchWrites > 100;
    }

    public boolean isEmpty() {
        return referenceWrites.isEmpty();
    }

    private int getNumberOfWriteItems() {
        return referenceWrites.size() + referenceCountIncrementByFilename.size();
    }

    private boolean isFileUpdated(String filename) {
        return referenceCountIncrementByFilename.containsKey(filename);
    }

    public List<SplitFileReferenceRequest> getRequests() {
        return requests;
    }

    public List<TransactWriteItem> getReferenceWrites() {
        return referenceWrites;
    }

    public Map<String, Integer> getReferenceCountIncrementByFilename() {
        return referenceCountIncrementByFilename;
    }
}
