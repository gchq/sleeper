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
package sleeper.clients.deploy.documentation;

import sleeper.clients.util.tablewriter.TableFieldDefinition;
import sleeper.clients.util.tablewriter.TableStructure;
import sleeper.clients.util.tablewriter.TableWriter;
import sleeper.clients.util.tablewriter.TableWriterFactory;
import sleeper.core.deploy.DockerDeployment;
import sleeper.core.properties.SleeperProperty;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;

public class GenerateJarsDocumentation {

    private GenerateJarsDocumentation() {
    }

    public static void main(String[] args) throws IOException {
        System.out.println(args[0]);
        Path headPath = Path.of(args[0]).resolve("docs/deployment/jars-to-upload");
        if (Files.exists(headPath)) {
            Files.delete(headPath);
        }
        Files.createFile(headPath);
        writeFile(headPath, output -> writePropertiesMarkdownFile(output, "Deployment Jars",
                createTableWriter(DockerDeployment.all())));
    }

    private static TableWriter createTableWriter(List<DockerDeployment> deployments) {
        TableFieldDefinition name = TableFieldDefinition.field("Property Name");
        TableFieldDefinition optionalStack = TableFieldDefinition.field("Optional Stack");
        TableFieldDefinition multiplatform = TableFieldDefinition.field("Multiplatform");

        TableWriterFactory factory = TableWriterFactory.builder()
                .structure(TableStructure.MARKDOWN_FORMAT)
                .addFields(name, optionalStack, multiplatform)
                .build();

        return factory.tableBuilder()
                .itemsAndWriter(deployments, (deployment, row) -> {
                    row.value(name, deployment.getDeploymentName());
                    row.value(optionalStack, deployment.getOptionalStack());
                    row.value(multiplatform, deployment.isMultiplatform());
                }).build();
    }

    private static <T extends SleeperProperty> void writePropertiesMarkdownFile(
            OutputStream output, String groupNamePrefix, TableWriter tableWriter) {
        PrintStream out = printStream(output);
        out.println("## " + groupNamePrefix);
        out.println();
        tableWriter.write(out);
    }

    private static void writeFile(Path file, Consumer<OutputStream> generator) {
        try (OutputStream stream = Files.newOutputStream(file)) {
            generator.accept(stream);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed writing file: " + file, e);
        }
    }

    private static PrintStream printStream(OutputStream output) {
        try {
            return new PrintStream(output, true, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new UncheckedIOException(e);
        }
    }
}
