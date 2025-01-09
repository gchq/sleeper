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
package sleeper.core.statestore.transactionlog;

/**
 * Holds a transaction that should be added to the log. The transaction may or may not already be held in S3.
 */
public class AddTransactionRequest {

    private final String bucketName;
    private final String key;
    private final StateStoreTransaction<?> transaction;

    private AddTransactionRequest(String bucketName, String key, StateStoreTransaction<?> transaction) {
        this.bucketName = bucketName;
        this.key = key;
        this.transaction = transaction;
    }

    /**
     * Creates a request to add file transaction that's held in S3.
     *
     * @param  bucketName  the bucket
     * @param  key         the object key
     * @param  transaction the transaction object
     * @return             the request
     */
    public static AddTransactionRequest fileTransactionInBucket(String bucketName, String key, FileReferenceTransaction transaction) {
        return new AddTransactionRequest(bucketName, key, transaction);
    }

}
