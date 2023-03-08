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

package sleeper.clients.admin;

import sleeper.configuration.properties.SleeperProperties;
import sleeper.configuration.properties.SleeperProperty;

import java.util.ArrayList;
import java.util.List;

public class PropertiesDiff<T extends SleeperProperty> {
    private final SleeperProperties<T> properties1;
    private final SleeperProperties<T> properties2;
    private final List<PropertyDiff> propertyDiffs;

    public PropertiesDiff(SleeperProperties<T> properties1, SleeperProperties<T> properties2) {
        this.properties1 = properties1;
        this.properties2 = properties2;
        this.propertyDiffs = new ArrayList<>();
        calculateDiffs();
    }

    private void calculateDiffs() {
    }

    public boolean isChanged() {
        return !properties1.equals(properties2);
    }

    public List<PropertyDiff> getChanges() {
        return List.of();
    }
}
