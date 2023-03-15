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
import sleeper.console.ConsoleOutput;

public class UpdatePropertiesRequest<T extends SleeperProperties<?>> {

    private final PropertiesDiff diff;
    private final T updatedProperties;

    public UpdatePropertiesRequest(PropertiesDiff diff, T updatedProperties) {
        this.diff = diff;
        this.updatedProperties = updatedProperties;
    }

    public PropertiesDiff getDiff() {
        return diff;
    }

    public T getUpdatedProperties() {
        return updatedProperties;
    }

    public void printIfChanged(ConsoleOutput out) {
        diff.printIfChanged(out, updatedProperties.getPropertiesIndex());
    }
}
