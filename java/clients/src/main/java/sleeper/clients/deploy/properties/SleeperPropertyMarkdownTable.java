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
package sleeper.clients.deploy.properties;

import sleeper.clients.util.tablewriter.TableField;
import sleeper.clients.util.tablewriter.TableRow.Builder;
import sleeper.clients.util.tablewriter.TableStructure;
import sleeper.clients.util.tablewriter.TableWriter;
import sleeper.clients.util.tablewriter.TableWriterFactory;
import sleeper.core.properties.SleeperProperty;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

public class SleeperPropertyMarkdownTable {

    private SleeperPropertyMarkdownTable() {
    }

    public static <T extends SleeperProperty> TableWriter generateTableBuildForGroup(Collection<T> properties) {
        TableWriterFactory.Builder factoryBuilder = TableWriterFactory.builder().structure(TableStructure.MARKDOWN_FORMAT);
        factoryBuilder.addFields(SleeperPropertyMarkdownTable.getMarkdownFields());
        TableWriter.Builder builder = factoryBuilder.build().tableBuilder();
        properties.forEach(property -> {
            builder.row(generatePropertyDetails(factoryBuilder.getFields(), property));
        });

        return builder.build();
    }

    private static List<String> getMarkdownFields() {
        return List.of("Property Name", "Description", "Default Value", "Run CdkDeploy When Changed");
    }

    private static Consumer<Builder> generatePropertyDetails(List<TableField> fields, SleeperProperty property) {
        return row -> {
            row.value(fields.get(0), property.getPropertyName());
            row.value(fields.get(1), adjustLongEntryForMarkdown(property.getDescription()));
            row.value(fields.get(2), property.getDefaultValue());
            row.value(fields.get(3), property.isRunCdkDeployWhenChanged());
        };
    }

    private static String adjustLongEntryForMarkdown(String valueIn) {
        return valueIn.replaceAll("\n", "<br>");
    }
}
