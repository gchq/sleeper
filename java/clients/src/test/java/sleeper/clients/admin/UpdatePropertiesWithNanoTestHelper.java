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

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.SleeperProperties;
import sleeper.configuration.properties.SleeperProperty;
import sleeper.util.RunCommand;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

import static sleeper.configuration.properties.PropertiesUtils.loadProperties;
import static sleeper.utils.RunCommandTestHelper.commandRunOn;

public class UpdatePropertiesWithNanoTestHelper {
    private final Path tempDir;
    private final Path expectedPropertiesFile;

    public UpdatePropertiesWithNanoTestHelper(Path tempDir) {
        this.tempDir = tempDir;
        this.expectedPropertiesFile = tempDir.resolve("sleeper/admin/temp.properties");
    }

    public String[] updateInstancePropertiesGetCommandRun(InstanceProperties properties) throws Exception {
        return commandRunOn(runCommand ->
                updateProperties(properties, runCommand));
    }

    public InstanceProperties updateInstancePropertiesGetPropertiesWritten(InstanceProperties properties) throws Exception {
        AtomicReference<InstanceProperties> foundProperties = new AtomicReference<>();
        updateProperties(properties, command -> {
            foundProperties.set(new InstanceProperties(loadProperties(expectedPropertiesFile)));
            return 0;
        });
        return foundProperties.get();
    }

    public <T extends SleeperProperty> UpdatePropertiesRequest updateProperties(
            SleeperProperties<T> before, SleeperProperties<T> after) throws IOException, InterruptedException {
        return updateProperties(before, command -> {
            after.save(expectedPropertiesFile);
            return 0;
        });
    }

    public <T extends SleeperProperty> UpdatePropertiesRequest updateProperties(
            SleeperProperties<T> properties, RunCommand runCommand) throws IOException, InterruptedException {
        return new UpdatePropertiesWithNano(tempDir, runCommand).updateProperties(properties);
    }
}
