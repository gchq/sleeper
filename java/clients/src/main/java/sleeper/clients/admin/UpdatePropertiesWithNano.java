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
import sleeper.configuration.properties.table.TableProperties;
import sleeper.util.ClientUtils;
import sleeper.util.RunCommand;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static sleeper.configuration.properties.PropertiesUtils.loadProperties;

public class UpdatePropertiesWithNano {

    private final Path tempDirectory;
    private final RunCommand runCommand;

    public UpdatePropertiesWithNano(Path tempDirectory) {
        this(tempDirectory, ClientUtils::runCommand);
    }

    public UpdatePropertiesWithNano(Path tempDirectory, RunCommand runCommand) {
        this.tempDirectory = tempDirectory;
        this.runCommand = runCommand;
    }

    public UpdatePropertiesRequest<InstanceProperties> openPropertiesFile(InstanceProperties properties) throws IOException, InterruptedException {
        InstanceProperties updatedProperties = new InstanceProperties(editProperties(properties));
        return buildRequest(properties, updatedProperties);
    }

    public UpdatePropertiesRequest<TableProperties> openPropertiesFile(TableProperties properties) throws IOException, InterruptedException {
        TableProperties updatedProperties = TableProperties.reinitialise(properties, editProperties(properties));
        return buildRequest(properties, updatedProperties);
    }

    private <T extends SleeperProperty> Properties editProperties(SleeperProperties<T> properties) throws IOException, InterruptedException {
        Files.createDirectories(tempDirectory.resolve("sleeper/admin"));
        Path propertiesFile = tempDirectory.resolve("sleeper/admin/temp.properties");
        try (BufferedWriter writer = Files.newBufferedWriter(propertiesFile)) {
            properties.saveUsingPrettyPrinter(new PrintWriter(writer));
        }
        runCommand.run("nano", propertiesFile.toString());
        return loadProperties(propertiesFile);
    }

    private <T extends SleeperProperties<?>> UpdatePropertiesRequest<T> buildRequest(T before, T after) {
        return new UpdatePropertiesRequest<>(new PropertiesDiff(before.toMap(), after.toMap()), after);
    }
}
