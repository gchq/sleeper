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
import sleeper.configuration.properties.local.LoadLocalProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;

import java.nio.file.Path;

public class ReadTableProperty {

    private ReadTableProperty() {
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            throw new IllegalArgumentException("Usage: <configuration directory> <table name> <property name>");
        }
        Path basePath = Path.of(args[0]);
        String tableName = args[1];
        String propertyName = args[2];
        InstanceProperties instanceProperties = LoadLocalProperties.loadInstancePropertiesFromDirectory(basePath);
        TableProperties tableProperties = LoadLocalProperties.loadTableFromDirectory(tableName, instanceProperties, basePath);
        System.out.println(tableProperties.get(TableProperty.fromName(propertyName)));
    }
}
