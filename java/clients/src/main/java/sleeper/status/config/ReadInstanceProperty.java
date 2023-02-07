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
package sleeper.status.config;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.InstanceProperty;
import sleeper.configuration.properties.local.LoadLocalProperties;

import java.nio.file.Path;

public class ReadInstanceProperty {

    private ReadInstanceProperty() {
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            throw new IllegalArgumentException("Usage: <configuration directory> <property name>");
        }
        Path basePath = Path.of(args[0]);
        String propertyName = args[1];
        InstanceProperties instanceProperties = LoadLocalProperties.loadInstancePropertiesFromDirectory(basePath);
        System.out.println(instanceProperties.get(InstanceProperty.fromName(propertyName)));
    }
}
