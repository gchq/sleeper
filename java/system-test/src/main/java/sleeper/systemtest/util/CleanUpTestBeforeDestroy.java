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
package sleeper.systemtest.util;

import sleeper.configuration.properties.local.LoadLocalProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.status.update.CleanUpBeforeDestroy;
import sleeper.systemtest.SystemTestProperties;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.systemtest.SystemTestProperty.SYSTEM_TEST_CLUSTER_NAME;

public class CleanUpTestBeforeDestroy {

    private CleanUpTestBeforeDestroy() {
    }

    public static void main(String[] args) {
        if (1 != args.length) {
            throw new IllegalArgumentException("Usage: <configuration directory>");
        }
        Path baseDir = Path.of(args[0]);
        SystemTestProperties instanceProperties = LoadLocalProperties.loadInstancePropertiesFromDirectory(new SystemTestProperties(), baseDir);
        List<TableProperties> tablePropertiesList = LoadLocalProperties
                .loadTablesFromDirectory(instanceProperties, baseDir).collect(Collectors.toList());

        CleanUpBeforeDestroy.cleanUp(instanceProperties, tablePropertiesList,
                List.of(instanceProperties.get(SYSTEM_TEST_CLUSTER_NAME)));
    }
}
