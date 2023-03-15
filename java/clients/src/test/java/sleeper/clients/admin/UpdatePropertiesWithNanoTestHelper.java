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
import sleeper.configuration.properties.table.TableProperties;
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
                updater(runCommand).updateProperties(properties));
    }

    public InstanceProperties updateInstancePropertiesGetPropertiesWritten(InstanceProperties properties) throws Exception {
        AtomicReference<InstanceProperties> foundProperties = new AtomicReference<>();
        updater(command -> {
            foundProperties.set(new InstanceProperties(loadProperties(expectedPropertiesFile)));
            return 0;
        }).updateProperties(properties);
        return foundProperties.get();
    }

    public Path updateInstancePropertiesGetPathToFile(InstanceProperties properties) throws IOException, InterruptedException {
        updater(command -> 0).updateProperties(properties);
        return expectedPropertiesFile;
    }

    public UpdatePropertiesRequest updateProperties(
            InstanceProperties before, InstanceProperties after) throws IOException, InterruptedException {
        return updaterSavingProperties(after).updateProperties(before);
    }

    public UpdatePropertiesRequest updateProperties(
            TableProperties before, TableProperties after) throws IOException, InterruptedException {
        return updaterSavingProperties(after).updateProperties(before);
    }

    private UpdatePropertiesWithNano updaterSavingProperties(SleeperProperties<?> after) {
        return updater(command -> {
            after.save(expectedPropertiesFile);
            return 0;
        });
    }

    private UpdatePropertiesWithNano updater(RunCommand runCommand) {
        return new UpdatePropertiesWithNano(tempDir, runCommand);
    }
}
