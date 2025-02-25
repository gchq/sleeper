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
package sleeper.core.statestore.transactionlog.log;

import java.util.Objects;
import java.util.Optional;

/**
 * The result of uploading a transaction body if it was necessary. The transaction may have been stored, and may have
 * been serialised without being stored.
 */
public class StoreTransactionBodyResult {

    private final String bodyKey;
    private final String serialisedTransaction;

    private StoreTransactionBodyResult(String bodyKey, String serialisedTransaction) {
        this.bodyKey = bodyKey;
        this.serialisedTransaction = serialisedTransaction;
    }

    /**
     * Returns a result where the transaction was stored in the data bucket.
     *
     * @param  bodyKey the object key in the data bucket
     * @return         the result
     */
    public static StoreTransactionBodyResult stored(String bodyKey) {
        return new StoreTransactionBodyResult(bodyKey, null);
    }

    /**
     * Returns a result where the transaction was serialised, but not stored.
     *
     * @param  serialisedTransaction the transaction serialised as a string
     * @return                       the result
     */
    public static StoreTransactionBodyResult notStored(String serialisedTransaction) {
        return new StoreTransactionBodyResult(null, serialisedTransaction);
    }

    /**
     * Returns a result where the transaction was not stored.
     *
     * @return the result
     */
    public static StoreTransactionBodyResult notStored() {
        return new StoreTransactionBodyResult(null, null);
    }

    public Optional<String> getBodyKey() {
        return Optional.ofNullable(bodyKey);
    }

    public Optional<String> getSerialisedTransaction() {
        return Optional.ofNullable(serialisedTransaction);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bodyKey);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof StoreTransactionBodyResult)) {
            return false;
        }
        StoreTransactionBodyResult other = (StoreTransactionBodyResult) obj;
        return Objects.equals(bodyKey, other.bodyKey);
    }

    @Override
    public String toString() {
        return "StoreTransactionBodyResult{bodyKey=" + bodyKey + "}";
    }
}
