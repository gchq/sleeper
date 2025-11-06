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

package sleeper.clients.deploy.container;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class InMemoryEcrRepositories implements CheckVersionExistsInEcr {
    private final Map<String, Set<String>> versionsByRepositoryName = new HashMap<>();

    public void addVersionToRepository(String repository, String version) {
        versionsByRepositoryName.computeIfAbsent(repository, r -> new HashSet<>()).add(version);
    }

    @Override
    public boolean versionExistsInRepository(String repository, String version) {
        return Optional.ofNullable(versionsByRepositoryName.get(repository))
                .map(versions -> versions.contains(version))
                .orElse(false);
    }
}
