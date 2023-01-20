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

package sleeper.build.github.containers;

import sleeper.build.github.api.GitHubApi;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;

import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Pattern;

public class DeleteGHCRImages {
    private final GitHubApi api;
    private final String organization;
    private final String imageName;
    private final Pattern tagsPattern;

    private DeleteGHCRImages(Builder builder) {
        api = builder.api;
        organization = builder.organization;
        imageName = builder.imageName;
        tagsPattern = builder.tagsPattern;
    }

    public static Builder withApi(GitHubApi api) {
        return new Builder().api(api);
    }

    public void delete() {
        for (GitHubPackageVersionResponse version : getAllVersions()) {
            deleteVersion(version.getId());
        }
    }

    private List<GitHubPackageVersionResponse> getAllVersions() {
        WebTarget target = containerPath(imageName).path("versions");
        return api.request(target).get(new GenericType<>() {
        });
    }

    private void deleteVersion(String versionId) {
        WebTarget target = containerPath(imageName).path("versions").path(versionId);
        api.request(target).delete(Void.class);
    }

    private WebTarget containerPath(String packageName) {
        return packagesBasePath().path("container").path(packageName);
    }

    private WebTarget packagesBasePath() {
        return api.path("orgs").path(organization).path("packages");
    }

    public static final class Builder {
        private GitHubApi api;
        private String organization;
        private String imageName;
        private Pattern tagsPattern;

        private Builder() {
        }

        public Builder api(GitHubApi api) {
            this.api = api;
            return this;
        }

        public Builder organization(String organization) {
            this.organization = organization;
            return this;
        }

        public Builder imageName(String imageName) {
            this.imageName = imageName;
            return this;
        }

        public Builder tagsPattern(Pattern tagsPattern) {
            this.tagsPattern = tagsPattern;
            return this;
        }

        public Builder tagsPattern(String tagsPattern) {
            return tagsPattern(Pattern.compile(tagsPattern));
        }

        public Builder applyMutation(Consumer<Builder> consumer) {
            consumer.accept(this);
            return this;
        }

        public void delete() {
            build().delete();
        }

        public DeleteGHCRImages build() {
            return new DeleteGHCRImages(this);
        }
    }
}
