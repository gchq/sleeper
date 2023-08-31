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

package sleeper.clients.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class EcrRepositoriesInMemory implements EcrRepositoryCreator.Client {
    private final Map<String, Set<String>> versionsByRepositoryName = new HashMap<>();
    private final Set<String> repositoriesWithEmrServerlessPolicy = new HashSet<>();

    @Override
    public boolean repositoryExists(String repository) {
        return versionsByRepositoryName.containsKey(repository);
    }

    @Override
    public void createRepository(String repository) {
        if (repositoryExists(repository)) {
            throw new IllegalArgumentException("Repository already exists: " + repository);
        }
        versionsByRepositoryName.put(repository, new HashSet<>());
    }

    public void addVersionToRepository(String repository, String version) {
        versionsByRepositoryName.get(repository).add(version);
    }

    @Override
    public void deleteRepository(String repository) {
        versionsByRepositoryName.remove(repository);
    }

    @Override
    public void createEmrServerlessAccessPolicy(String repository) {
        if (repositoriesWithEmrServerlessPolicy.contains(repository)) {
            throw new IllegalArgumentException("Repository already has EMR Serverless policy: " + repository);
        }
        repositoriesWithEmrServerlessPolicy.add(repository);
    }

    @Override
    public boolean versionExistsInRepository(String repository, String version) {
        return versionsByRepositoryName.get(repository).contains(version);
    }

    public Collection<String> getRepositories() {
        return versionsByRepositoryName.keySet();
    }

    public Collection<String> getRepositoriesWithEmrServerlessPolicy() {
        return repositoriesWithEmrServerlessPolicy;
    }
}
