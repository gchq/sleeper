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

public class GenerateDockerImageDocumentation {

    private GenerateDockerImageDocumentation() {
    }

    public static void main(String[] args) throws IOException {
        Path path = Path.of(args[0]).resolve("docs/deployment/images-to-upload.md");
        if (Files.exists(path)) {
            Files.delete(path);
        }
        Files.createFile(path);
        writeFile(path, output -> writePropertiesMarkdownFile(output, "Docker Deployment Images", getECRDescription(),
                createDockerDeploymentTableWriter(DockerDeployment.all())));
        writeFile(path, output -> writePropertiesMarkdownFile(output, "Lambda Deployment Images", getLambdaDescription(),
                createLambdaJarTableWriter(LambdaJar.getAll())));
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

    private static void writePropertiesMarkdownFile(
            OutputStream output, String sectionName, String sectionDescription, TableWriter tableWriter) {
        PrintStream out = printStream(output);
        out.println("## " + sectionName);
        out.println(sectionDescription);
        out.println();
        tableWriter.write(out);
        out.println();
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

    private static String getECRDescription() {
        return """
                These are the Docker deployment images.<br>
                A build of Sleeper outputs several directories under scripts/docker. Each is the directory to build a Docker image, with a Dockerfile.
                Some of these are used for parts of Sleeper that are always deployed from Docker images, and those are listed here.<br>
                *Deployment name - This is both the name of its directory under scripts/docker, and the name of the image when it's built and the repository it's uploaded to.<br>
                *Optional Stack - They're each associated with an optional stack, and will only be used when that optional stack is deployed in an instance of Sleeper.<br>
                *Multiplatform - Compaction job execution is built as a multiplatform image, so it can be deployed in both x86 and ARM architectures.
                """;
    }

    private static String getLambdaDescription() {
        return """
                These are the Lambda deployment images.<br>
                These are all used with the Docker build directory that's output during a build of Sleeper at scripts/docker/lambda.
                Most lambdas are usually deployed from a jar in the jars bucket, but some need to be deployed as a Docker container, and we have the option to deploy all lambdas as Docker containers as well.
                To build a Docker image for a lambda, we copy its jar file from scripts/jars to scripts/docker/lambda/lambda.jar, and then run the Docker build for that directory.
                This results in a separate Docker image for each lambda jar.<br>
                *Filename - This is the name of the jar file that's output by the build in scripts/jars.
                It includes the version number you've built, which we've included as a placeholder here.<br>
                *Image name - This is the name of the Docker image that's built, and the name of the repository it's uploaded to.<br>
                *Always Docker deploy - This means that that lambda will always be deployed with Docker, usually because the jar is too large to deploy directly.
                    """;
    }
}
