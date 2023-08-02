/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.build.maven;

import java.util.Objects;

public class ArtifactReference {
    private final String groupId;
    private final String artifactId;

    private ArtifactReference(String groupId, String artifactId) {
        this.groupId = Objects.requireNonNull(groupId, "groupId must not be null");
        this.artifactId = Objects.requireNonNull(artifactId, "artifactId must not be null");
    }

    public static ArtifactReference groupAndArtifact(String groupId, String artifactId) {
        return new ArtifactReference(groupId, artifactId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ArtifactReference that = (ArtifactReference) o;
        return groupId.equals(that.groupId) && artifactId.equals(that.artifactId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, artifactId);
    }

    @Override
    public String toString() {
        return groupId + ":" + artifactId;
    }

    public String getArtifactId() {
        return artifactId;
    }
}
