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
import java.util.UUID;

/**
 * A pointer to a revision of Sleeper table state held in S3. This is indexed in DynamoDB.
 */
public class S3RevisionId {
    private static final String FIRST_REVISION = getFirstRevisionNumber();
    private final String revision;
    private final String uuid;

    S3RevisionId(String revision, String uuid) {
        this.revision = revision;
        this.uuid = uuid;
    }

    static S3RevisionId firstRevision(String uuid) {
        return new S3RevisionId(FIRST_REVISION, uuid);
    }

    S3RevisionId getNextRevisionId() {
        String revision = this.revision;
        while (revision.startsWith("0")) {
            revision = revision.substring(1);
        }
        long revisionNumber = Long.parseLong(revision);
        long nextRevisionNumber = revisionNumber + 1;
        StringBuilder nextRevision = new StringBuilder("" + nextRevisionNumber);
        while (nextRevision.length() < 12) {
            nextRevision.insert(0, "0");
        }
        return new S3RevisionId(nextRevision.toString(), UUID.randomUUID().toString());
    }

    public String getRevision() {
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

    private static String getFirstRevisionNumber() {
        StringBuilder versionString = new StringBuilder("1");
        while (versionString.length() < 12) {
            versionString.insert(0, "0");
        }
        return versionString.toString();
    }
}
