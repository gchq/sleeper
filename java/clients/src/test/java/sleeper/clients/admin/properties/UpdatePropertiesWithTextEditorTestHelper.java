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

package sleeper.clients.admin.properties;

import sleeper.clients.util.command.CommandPipeline;
import sleeper.clients.util.command.CommandRunner;
import sleeper.core.properties.PropertyGroup;
import sleeper.core.properties.SleeperProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static sleeper.clients.testutil.RunCommandTestHelper.recordCommandsRun;
import static sleeper.clients.testutil.RunCommandTestHelper.singleCommand;
import static sleeper.core.properties.PropertiesUtils.loadProperties;

public class UpdatePropertiesWithTextEditorTestHelper {
    private final Path tempDir;
    private final Path expectedPropertiesFile;
    private final Map<String, String> environmentVariables = new HashMap<>();

    public UpdatePropertiesWithTextEditorTestHelper(Path tempDir) {
        this.tempDir = tempDir;
        this.expectedPropertiesFile = tempDir.resolve("sleeper/admin/temp.properties");
    }

    public String[] openInstancePropertiesGetCommandRun(InstanceProperties properties) throws Exception {
        List<CommandPipeline> commandsThatRan = new ArrayList<>();
        updaterWithCommandHandler(recordCommandsRun(commandsThatRan)).openPropertiesFile(properties);
        return singleCommand(commandsThatRan);
    }

    public InstanceProperties openInstancePropertiesGetPropertiesWritten(InstanceProperties properties) throws Exception {
        return InstanceProperties.createWithoutValidation(
                openFileGetPropertiesWritten(updater -> updater.openPropertiesFile(properties)));
    }

    public Path openInstancePropertiesGetPathToFile(InstanceProperties properties) throws Exception {
        return openFileGetPathToFile(updater -> updater.openPropertiesFile(properties));
    }

    public UpdatePropertiesRequest<InstanceProperties> updateProperties(
            InstanceProperties before, InstanceProperties after) throws Exception {
        return updaterSavingProperties(after).openPropertiesFile(before);
    }

    public UpdatePropertiesRequest<InstanceProperties> updatePropertiesWithGroup(
            InstanceProperties before, String toWriteInEditor, PropertyGroup group) throws Exception {
        return updaterWithCommandHandler(command -> {
            Files.writeString(expectedPropertiesFile, toWriteInEditor);
            return 0;
        }).openPropertiesFile(before, group);
    }

    public UpdatePropertiesRequest<TableProperties> updateProperties(
            TableProperties before, TableProperties after) throws Exception {
        return updaterSavingProperties(after).openPropertiesFile(before);
    }

    public UpdatePropertiesRequest<TableProperties> updatePropertiesWithGroup(
            TableProperties before, String toWriteInEditor, PropertyGroup group) throws Exception {
        return updaterWithCommandHandler(command -> {
            Files.writeString(expectedPropertiesFile, toWriteInEditor);
            return 0;
        }).openPropertiesFile(before, group);
    }

    @FunctionalInterface
    public interface OpenFile<T extends SleeperProperties<?>> {
        UpdatePropertiesRequest<T> open(UpdatePropertiesWithTextEditor updater) throws IOException, InterruptedException;
    }

    public <T extends SleeperProperties<?>> Properties openFileGetPropertiesWritten(OpenFile<T> openFile) throws Exception {
        AtomicReference<Properties> foundProperties = new AtomicReference<>();
        openFile.open(updaterWithCommandHandler(command -> {
            foundProperties.set(loadProperties(expectedPropertiesFile));
            return 0;
        }));
        return foundProperties.get();
    }

    public <T extends SleeperProperties<?>> Path openFileGetPathToFile(OpenFile<T> openFile) throws Exception {
        openFile.open(updaterWithCommandHandler(command -> 0));
        return expectedPropertiesFile;
    }

    public void setEnvironmentVariable(String name, String value) {
        environmentVariables.put(name, value);
    }

    private UpdatePropertiesWithTextEditor updaterSavingProperties(SleeperProperties<?> after) {
        return updaterWithCommandHandler(command -> {
            after.save(expectedPropertiesFile);
            return 0;
        });
    }

    private UpdatePropertiesWithTextEditor updaterWithCommandHandler(CommandRunner runCommand) {
        return new UpdatePropertiesWithTextEditor(tempDir, runCommand, environmentVariables::get);
    }
}
