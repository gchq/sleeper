/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.configuration.properties;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SleeperPropertyIndex<T extends SleeperProperty> {

    private final Map<String, T> allMap = new HashMap<>();
    private final Map<PropertyGroup, List<T>> byGroup = new HashMap<>();
    private final List<T> all = new ArrayList<>();
    private final List<T> userDefined = new ArrayList<>();
    private final List<T> cdkDefined = new ArrayList<>();

    public void add(T property) {
        allMap.put(property.getPropertyName(), property);
        all.add(property);
        byGroup.computeIfAbsent(property.getPropertyGroup(), group -> new ArrayList<>())
                .add(property);
        if (property.isSetByCdk()) {
            cdkDefined.add(property);
        }
        if (property.isUserDefined()) {
            userDefined.add(property);
        }
    }

    public void addAll(Collection<? extends T> properties) {
        properties.forEach(this::add);
    }

    public List<T> getAll() {
        return Collections.unmodifiableList(all);
    }

    public List<T> getUserDefined() {
        return Collections.unmodifiableList(userDefined);
    }

    public List<T> getCdkDefined() {
        return Collections.unmodifiableList(cdkDefined);
    }

    public Optional<T> getByName(String propertyName) {
        return Optional.ofNullable(allMap.get(propertyName));
    }

    public List<T> getAllInGroup(PropertyGroup propertyGroup) {
        return byGroup.getOrDefault(propertyGroup, Collections.emptyList());
    }
}
