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

package sleeper.statestore.s3;

import java.util.Objects;

class S3RevisionId {
    private final String revision;
    private final String uuid;

    S3RevisionId(String revision, String uuid) {
        this.revision = revision;
        this.uuid = uuid;
    }

    String getRevision() {
        return revision;
    }

    String getUuid() {
        return uuid;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof S3RevisionId)) {
            return false;
        }
        S3RevisionId that = (S3RevisionId) o;
        return Objects.equals(revision, that.revision) && Objects.equals(uuid, that.uuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(revision, uuid);
    }

    @Override
    public String toString() {
        return "RevisionId{" +
                "revision='" + revision + '\'' +
                ", uuid='" + uuid + '\'' +
                '}';
    }
}
