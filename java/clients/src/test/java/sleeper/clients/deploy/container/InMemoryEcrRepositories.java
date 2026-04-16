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

package sleeper.clients.deploy.container;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class InMemoryEcrRepositories implements CheckDigestExistsInEcr {
    private final Map<String, Set<String>> digestsByRepositoryName = new HashMap<>();

    public void addDigestToRepository(String repository, String digest) {
        digestsByRepositoryName.computeIfAbsent(repository, r -> new HashSet<>()).add(digest);
    }

    @Override
    public boolean digestExistsInRepository(String repository, String digest) {
        return Optional.ofNullable(digestsByRepositoryName.get(repository))
                .map(digests -> digests.contains(digest))
                .orElse(false);
    }
}
