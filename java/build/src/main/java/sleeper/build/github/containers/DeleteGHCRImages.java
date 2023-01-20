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

    public void deleteAllImages() {
        // getAllContainers
        List<GitHubPackageResponse> allContainers = getAllContainers();
        // for each, getAllImages and delete
    }

    private List<GitHubPackageResponse> getAllContainers() {
        ///orgs/test-org/packages?package_type=container
        WebTarget target = api.path("orgs").path(organization).path("packages")
                .queryParam("package_type", "container");
        List<GitHubPackageResponse> response = api.request(target).get(new GenericType<List<GitHubPackageResponse>>() {
        });
        return response;
    }
}
