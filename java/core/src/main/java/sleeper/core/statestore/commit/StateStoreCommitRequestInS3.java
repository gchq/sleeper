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
package sleeper.core.statestore.commit;

import java.util.Objects;

/**
 * A state store commit request stored in S3.
 */
public class StateStoreCommitRequestInS3 {
    private final String keyInS3;

    public StateStoreCommitRequestInS3(String keyInS3) {
        this.keyInS3 = keyInS3;
    }

    /**
     * Creates a key for a file in S3, to hold a commit request. Used when uploading a commit request to S3. This should
     * be held in the Sleeper instance data bucket.
     *
     * @param  tableId  the Sleeper table unique ID
     * @param  filename the filename without file type, can be a random UUID
     * @return          the S3 object key
     */
    public static String createFileS3Key(String tableId, String filename) {
        return tableId + "/statestore/commitrequests/" + filename + ".json";
    }

    public String getKeyInS3() {
        return keyInS3;
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyInS3);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof StateStoreCommitRequestInS3)) {
            return false;
        }
        StateStoreCommitRequestInS3 other = (StateStoreCommitRequestInS3) obj;
        return Objects.equals(keyInS3, other.keyInS3);
    }

    @Override
    public String toString() {
        return "StateStoreCommitRequestInS3{keyInS3=" + keyInS3 + "}";
    }

}
