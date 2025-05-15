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

import sleeper.clients.util.tablewriter.TableFieldDefinition;
import sleeper.clients.util.tablewriter.TableStructure;
import sleeper.clients.util.tablewriter.TableWriter;
import sleeper.clients.util.tablewriter.TableWriterFactory;
import sleeper.core.properties.SleeperProperty;

import java.util.List;

public class SleeperPropertyMarkdownTable {

    private SleeperPropertyMarkdownTable() {
    }

    private static final TableFieldDefinition NAME = TableFieldDefinition.field("Property Name");
    private static final TableFieldDefinition DESCRIPTION = TableFieldDefinition.field("Description");
    private static final TableFieldDefinition DEFAULT_VALUE = TableFieldDefinition.field("Default Value");
    private static final TableFieldDefinition RUN_CDK_DEPLOY_WHEN_CHANGED = TableFieldDefinition.field("Run CdkDeploy When Changed");

    public static <T extends SleeperProperty> TableWriter createTableWriterForUserDefinedProperties(List<T> properties) {
        return createTableWriter(properties);
    }

    public static <T extends SleeperProperty> TableWriter createTableWriterForCdkDefinedProperties(List<T> properties) {
        return createTableWriter(properties, DEFAULT_VALUE, RUN_CDK_DEPLOY_WHEN_CHANGED);
    }

    private static <T extends SleeperProperty> TableWriter createTableWriter(List<T> properties, TableFieldDefinition... hideFields) {
        TableWriterFactory factory = TableWriterFactory.builder()
                .structure(TableStructure.MARKDOWN_FORMAT)
                .addFields(NAME, DESCRIPTION, DEFAULT_VALUE, RUN_CDK_DEPLOY_WHEN_CHANGED)
                .build();
        return factory.tableBuilder()
                .showFields(false, hideFields)
                .itemsAndWriter(properties, (property, row) -> {
                    row.value(NAME, property.getPropertyName());
                    row.value(DESCRIPTION, adjustLongEntryForMarkdown(property.getDescription()));
                    row.value(DEFAULT_VALUE, property.getDefaultValue());
                    row.value(RUN_CDK_DEPLOY_WHEN_CHANGED, property.isRunCdkDeployWhenChanged());
                }).build();
    }

    private static String adjustLongEntryForMarkdown(String valueIn) {
        return valueIn.replaceAll("\n", "<br>");
    }
}
