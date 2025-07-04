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
import sleeper.core.deploy.LambdaJar;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.function.Consumer;

public class GenerateJarsDocumentation {

    private GenerateJarsDocumentation() {
    }

    public static void main(String[] args) throws IOException {
        Path path = Path.of(args[0]).resolve("docs/deployment/jars-to-upload.md");
        if (Files.exists(path)) {
            Files.delete(path);
        }
        Files.createFile(path);
        writeFile(path, output -> writePropertiesMarkdownFile(output, "Deployment Jars", "These are the docker deployment Jars",
                createDockerDeploymentTableWriter(DockerDeployment.all())));
        writeFile(path, output -> writePropertiesMarkdownFile(output, "Lambda Jars", "These are the Lambda deploy jars",
                createLambdaJarTableWriter(LambdaJar.ALL)));
    }

    private static TableWriter createDockerDeploymentTableWriter(List<DockerDeployment> deployments) {
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

    private static TableWriter createLambdaJarTableWriter(List<LambdaJar> deployments) {
        TableFieldDefinition fileName = TableFieldDefinition.field("File Name");
        TableFieldDefinition imageName = TableFieldDefinition.field("Image Name");
        TableFieldDefinition isAlwaysDockerDeploy = TableFieldDefinition.field("Always docker deploy");

        TableWriterFactory factory = TableWriterFactory.builder()
                .structure(TableStructure.MARKDOWN_FORMAT)
                .addFields(fileName, imageName, isAlwaysDockerDeploy)
                .build();

        return factory.tableBuilder()
                .itemsAndWriter(deployments, (deployment, row) -> {
                    row.value(fileName, deployment.getFilename());
                    row.value(imageName, deployment.getImageName());
                    row.value(isAlwaysDockerDeploy, deployment.isAlwaysDockerDeploy());
                }).build();
    }

    private static void writePropertiesMarkdownFile(
            OutputStream output, String sectionName, String sectionDescription, TableWriter tableWriter) {
        PrintStream out = printStream(output);
        out.println("## " + sectionName);
        out.println();
        out.println(sectionDescription);
        out.println();
        tableWriter.write(out);
    }

    private static void writeFile(Path file, Consumer<OutputStream> generator) {
        try (OutputStream stream = Files.newOutputStream(file, StandardOpenOption.APPEND)) {
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
