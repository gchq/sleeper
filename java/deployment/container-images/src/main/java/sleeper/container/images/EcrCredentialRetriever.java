/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.container.images;

import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.ecr.model.AuthorizationData;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

/**
 * Retrieves container registry credentials from Amazon ECR.
 */
public class EcrCredentialRetriever implements ContainerRegistryCredentials.Retriever {

    private final EcrClient ecrClient;

    public EcrCredentialRetriever(EcrClient ecrClient) {
        this.ecrClient = ecrClient;
    }

    @Override
    public Optional<ContainerRegistryCredentials> retrieve() {
        List<AuthorizationData> auths = ecrClient.getAuthorizationToken().authorizationData();
        if (auths.size() != 1) {
            throw new RuntimeException("Expected 1 authorization token data object from ECR, found " + auths.size());
        }
        AuthorizationData data = auths.get(0);
        String decoded = new String(Base64.getDecoder().decode(data.authorizationToken()), StandardCharsets.UTF_8);
        String[] parts = decoded.split(":");
        return Optional.of(new ContainerRegistryCredentials(parts[0], parts[1]));
    }

}
