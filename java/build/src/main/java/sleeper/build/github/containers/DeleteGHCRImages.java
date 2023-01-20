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

public class DeleteGHCRImages {
    private final GitHubApi api;
    private final String organization;

    public DeleteGHCRImages(GitHubApi api, String organization) {
        this.api = api;
        this.organization = organization;
    }

    public void deleteAll() {
        for (GitHubPackageResponse container : getAllContainers()) {
            for (GitHubPackageVersionResponse version : getAllVersionsByPackage(container.getName())) {
                deleteVersion(container.getName(), version.getId());
            }
        }
    }

    private List<GitHubPackageResponse> getAllContainers() {
        WebTarget target = packagesBasePath()
                .queryParam("package_type", "container");
        return api.request(target).get(new GenericType<>() {
        });
    }

    private List<GitHubPackageVersionResponse> getAllVersionsByPackage(String packageName) {
        WebTarget target = containerPath(packageName).path("versions");
        return api.request(target).get(new GenericType<>() {
        });
    }

    private void deleteVersion(String packageName, String versionId) {
        WebTarget target = containerPath(packageName).path("versions").path(versionId);
        api.request(target).delete(Void.class);
    }

    private WebTarget containerPath(String packageName) {
        return packagesBasePath().path("container").path(packageName);
    }

    private WebTarget packagesBasePath() {
        return api.path("orgs").path(organization).path("packages");
    }
}
