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

import com.google.common.io.CharStreams;

import sleeper.clients.util.tablewriter.TableFieldDefinition;
import sleeper.clients.util.tablewriter.TableStructure;
import sleeper.clients.util.tablewriter.TableWriter;
import sleeper.clients.util.tablewriter.TableWriterFactory;
import sleeper.core.deploy.DockerDeployment;
import sleeper.core.deploy.LambdaJar;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class GenerateDockerImageDocumentation {

    private GenerateDockerImageDocumentation() {
    }

    public static void main(String[] args) throws IOException {
        Path path = Path.of(args[0]).resolve("docs/deployment/docker-images.md");
        String template = getResourceAsString("docker-images.template.md");
        String dockerDeploymentTable = tableToString(createDockerDeploymentTableWriter(DockerDeployment.all()));
        String lambdaTable = tableToString(createLambdaJarTableWriter(LambdaJar.all()));
        String output = template
                .replace("%DOCKER_IMAGES_TABLE%", dockerDeploymentTable)
                .replace("%LAMBDA_IMAGES_TABLE%", lambdaTable);
        Files.writeString(path, output);
    }

    private static TableWriter createDockerDeploymentTableWriter(List<DockerDeployment> deployments) {
        TableFieldDefinition name = TableFieldDefinition.field("Deployment Name");
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
        TableFieldDefinition fileName = TableFieldDefinition.field("Filename");
        TableFieldDefinition imageName = TableFieldDefinition.field("Image Name");
        TableFieldDefinition isAlwaysDockerDeploy = TableFieldDefinition.field("Always Docker deploy");

        TableWriterFactory factory = TableWriterFactory.builder()
                .structure(TableStructure.MARKDOWN_FORMAT)
                .addFields(fileName, imageName, isAlwaysDockerDeploy)
                .build();

        return factory.tableBuilder()
                .itemsAndWriter(deployments, (deployment, row) -> {
                    row.value(fileName, String.format(deployment.getFilenameFormat(), "`<version-number>`"));
                    row.value(imageName, deployment.getImageName());
                    row.value(isAlwaysDockerDeploy, deployment.isAlwaysDockerDeploy());
                }).build();
    }

    private static String getResourceAsString(String path) throws IOException {
        try (Reader reader = new InputStreamReader(GenerateDockerImageDocumentation.class.getClassLoader().getResourceAsStream(path))) {
            return CharStreams.toString(reader);
        }
    }

    private static String tableToString(TableWriter tableWriter) {
        OutputStream outputStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(outputStream, false, StandardCharsets.UTF_8);
        tableWriter.write(printStream);
        printStream.flush();
        return outputStream.toString();
    }
}
