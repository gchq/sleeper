/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.build.github.api.containers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GitHubPackageVersionResponse {

    private final String id;
    private final Instant updatedAt;
    private final Metadata metadata;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public GitHubPackageVersionResponse(
            @JsonProperty("id") String id,
            @JsonProperty("updated_at") Instant updatedAt,
            @JsonProperty("metadata") Metadata metadata) {
        this.id = id;
        this.updatedAt = updatedAt;
        this.metadata = metadata;
    }

    public String getId() {
        return id;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public List<String> getTags() {
        return metadata.container.tags;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Metadata {
        private final ContainerMetadata container;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public Metadata(@JsonProperty("container") ContainerMetadata container) {
            this.container = container;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ContainerMetadata {
        private final List<String> tags;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public ContainerMetadata(@JsonProperty("tags") List<String> tags) {
            this.tags = tags;
        }
    }
}
