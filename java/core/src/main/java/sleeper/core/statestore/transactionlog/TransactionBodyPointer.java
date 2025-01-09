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

import java.util.Objects;

/**
 * A pointer to where the body of a transaction is held. Currently this can only be S3.
 */
public class TransactionBodyPointer {
    private final String bucketName;
    private final String key;

    public TransactionBodyPointer(String bucketName, String key) {
        this.bucketName = bucketName;
        this.key = key;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getKey() {
        return key;
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucketName, key);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof TransactionBodyPointer)) {
            return false;
        }
        TransactionBodyPointer other = (TransactionBodyPointer) obj;
        return Objects.equals(bucketName, other.bucketName) && Objects.equals(key, other.key);
    }

    @Override
    public String toString() {
        return "TransactionBodyPointer{bucketName=" + bucketName + ", key=" + key + "}";
    }
}
