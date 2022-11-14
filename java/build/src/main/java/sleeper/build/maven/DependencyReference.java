/*
 * Copyright 2022 Crown Copyright
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

public class DependencyReference {

    private final String artifactId;
    private final String groupId;

    public DependencyReference(String artifactId, String groupId) {
        this.artifactId = Objects.requireNonNull(artifactId, "artifactId must not be null");
        this.groupId = Objects.requireNonNull(groupId, "groupId must not be null");
    }

    public static DependencyReference groupAndArtifact(String groupId, String artifactId) {
        return new DependencyReference(artifactId, groupId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DependencyReference that = (DependencyReference) o;
        return artifactId.equals(that.artifactId) && groupId.equals(that.groupId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(artifactId, groupId);
    }

    @Override
    public String toString() {
        return groupId + ':' + artifactId;
    }
}
