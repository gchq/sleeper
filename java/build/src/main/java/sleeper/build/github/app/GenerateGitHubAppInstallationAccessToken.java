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

package sleeper.build.github.app;

import sleeper.build.github.api.GitHubApi;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;

import java.io.IOException;
import java.nio.file.Path;

public class GenerateGitHubAppInstallationAccessToken {

    private final GitHubApi api;

    public GenerateGitHubAppInstallationAccessToken(GitHubApi api) {
        this.api = api;
    }

    public String generateWithInstallationId(String installationId) {
        WebTarget target = api.path("/app/installations").path(installationId).path("access_tokens");
        return api.request(target)
                .post(Entity.text(""), InstallationAccessTokenResponse.class)
                .getToken();
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.err.println("Usage: <private key pem file> <app ID> <installation ID>");
            System.exit(1);
        }
        Path pemFile = Path.of(args[0]);
        String appId = args[1];
        String installationId = args[2];

        String jwt = GenerateGitHubAppJWT.withPemFileAndAppId(pemFile, appId);
        GitHubApi api = GitHubApi.withToken(jwt);
        String accessToken = new GenerateGitHubAppInstallationAccessToken(api)
                .generateWithInstallationId(installationId);
        System.out.println(accessToken);
    }
}
