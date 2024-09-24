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
package sleeper.clients.admin.properties;

import sleeper.clients.util.ClientUtils;
import sleeper.clients.util.CommandRunner;
import sleeper.configuration.properties.PropertyGroup;
import sleeper.configuration.properties.SleeperProperties;
import sleeper.configuration.properties.SleeperProperty;
import sleeper.configuration.properties.format.SleeperPropertiesPrettyPrinter;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.function.Function;

import static java.util.function.Predicate.not;
import static sleeper.configuration.properties.PropertiesUtils.loadProperties;

public class UpdatePropertiesWithTextEditor {

    private final Path tempDirectory;
    private final CommandRunner runCommand;
    private final UpdatePropertiesTextEditorCommand editorCommand = new UpdatePropertiesTextEditorCommand();

    public UpdatePropertiesWithTextEditor(Path tempDirectory) {
        this(tempDirectory, ClientUtils::runCommandInheritIO);
    }

    public UpdatePropertiesWithTextEditor(Path tempDirectory, CommandRunner runCommand) {
        this.tempDirectory = tempDirectory;
        this.runCommand = runCommand;
    }

    public UpdatePropertiesRequest<InstanceProperties> openPropertiesFile(InstanceProperties properties) throws IOException, InterruptedException {
        InstanceProperties updatedProperties = InstanceProperties.createWithoutValidation(
                editProperties(properties, SleeperPropertiesPrettyPrinter::forInstanceProperties));
        return buildRequest(properties, updatedProperties);
    }

    public UpdatePropertiesRequest<TableProperties> openPropertiesFile(TableProperties properties) throws IOException, InterruptedException {
        TableProperties updatedProperties = TableProperties.reinitialise(properties,
                editProperties(properties, SleeperPropertiesPrettyPrinter::forTableProperties));
        return buildRequest(properties, updatedProperties);
    }

    public UpdatePropertiesRequest<InstanceProperties> openPropertiesFile(
            InstanceProperties properties, PropertyGroup propertyGroup) throws IOException, InterruptedException {
        Properties after = editPropertiesAndMerge(properties, propertyGroup, writer -> SleeperPropertiesPrettyPrinter.forInstancePropertiesWithGroup(writer, propertyGroup));
        return buildRequest(properties, InstanceProperties.createWithoutValidation(after));
    }

    public UpdatePropertiesRequest<TableProperties> openPropertiesFile(
            TableProperties properties, PropertyGroup propertyGroup) throws IOException, InterruptedException {
        Properties after = editPropertiesAndMerge(properties, propertyGroup, writer -> SleeperPropertiesPrettyPrinter.forTablePropertiesWithGroup(writer, propertyGroup));
        return buildRequest(properties, TableProperties.reinitialise(properties, after));
    }

    private <T extends SleeperProperty> Properties editProperties(
            SleeperProperties<T> properties,
            Function<PrintWriter, SleeperPropertiesPrettyPrinter<T>> printer) throws IOException, InterruptedException {
        Files.createDirectories(tempDirectory.resolve("sleeper/admin"));
        Path propertiesFile = tempDirectory.resolve("sleeper/admin/temp.properties");
        try (BufferedWriter writer = Files.newBufferedWriter(propertiesFile)) {
            printer.apply(new PrintWriter(writer)).print(properties);
        }
        runCommand.run(editorCommand.getCommand(), propertiesFile.toString());
        return loadProperties(propertiesFile);
    }

    private <T extends SleeperProperty> Properties editPropertiesAndMerge(
            SleeperProperties<T> properties, PropertyGroup propertyGroup,
            Function<PrintWriter, SleeperPropertiesPrettyPrinter<T>> printer) throws IOException, InterruptedException {

        Properties after = new Properties();
        after.putAll(properties.getProperties());

        Properties edited = editProperties(properties, printer);
        after.putAll(edited);

        properties.getPropertiesIndex().getAllInGroup(propertyGroup)
                .stream().map(SleeperProperty::getPropertyName)
                .filter(not(edited::containsKey))
                .forEach(after::remove);
        return after;
    }

    private <T extends SleeperProperties<?>> UpdatePropertiesRequest<T> buildRequest(T before, T after) {
        return new UpdatePropertiesRequest<>(new PropertiesDiff(before, after), after);
    }
}
