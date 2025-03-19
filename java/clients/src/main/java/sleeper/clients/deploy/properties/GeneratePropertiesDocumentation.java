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

import sleeper.core.properties.PropertyGroup;
import sleeper.core.properties.instance.InstancePropertyGroup;
import sleeper.core.properties.table.TablePropertyGroup;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;

/**
 * Words go here.
 */
public class GeneratePropertiesDocumentation {

    private GeneratePropertiesDocumentation() {
    }

    public static void main(String[] args) throws IOException {
        //fromRepositoryPath(Path.of(args[0]));
        try {
            System.out.println(" >>> Called this here!: " + args[0]);
            generateDocumentation(Path.of(args[0]));
        } catch (Exception ex) {
            System.out.println("Exception: " + ex);
        }
    }

    public static void generateDocumentation(Path root) throws IOException {
        Path headPath = Files.createDirectories(root.resolve("docs/usage/properties"));

        //---------- Instance Properties ----------
        Path instancePath = Files.createDirectories(headPath.resolve("instance/"));
        InstancePropertyGroup.getAll().forEach(instancePropertyGroup -> {
            try {
                writeFile(instancePath.resolve(instancePropertyGroup.getName().toLowerCase().replace(" ", "_") + ".md"),
                        writer -> writeInstancePropertiesMarkdownTable(instancePropertyGroup, writer));
            } catch (IOException e) {
                System.out.println("Unable to write property file for group: " + instancePropertyGroup.getName());
            }
        });

        //---------- Table Properties ----------
        Path tablePath = Files.createDirectories(headPath.resolve("table/"));
        TablePropertyGroup.getAll().forEach(tablePropertyGroup -> {
            try {
                writeFile(instancePath.resolve(tablePropertyGroup.getName().toLowerCase().replace(" ", "_") + ".md"),
                        writer -> writeTablePropertiesMarkdownTable(tablePropertyGroup, writer));
            } catch (IOException e) {
                System.out.println("Unable to write property file for group: " + tablePropertyGroup.getName());
            }
        });
    }

    /**
     * Generates table containing all the instance properties for a given property group in markdown format.
     * This is then written out to a named file.
     *
     * @param group the group of properties
     * @param out   the writer for the files
     */
    public static void writeInstancePropertiesMarkdownTable(PropertyGroup group, Writer out) {
        PrintWriter writer = new PrintWriter(out);
        writer.println(">>>>>>>>>>>>>" + group.getName() + "<<<<<<<<<<");
    }

    /**
     * Generates table containing all the table properties for a given property group in markdown format.
     * This is then written out to a named file.
     *
     * @param group the group of properties
     * @param out   the writer for the file
     */
    public static void writeTablePropertiesMarkdownTable(PropertyGroup group, Writer out) {
        PrintWriter writer = new PrintWriter(out);
        writer.println(">>>>>>>>>>>>>" + group.getName() + "<<<<<<<<<<");
    }

    private static void writeFile(Path file, Consumer<Writer> generator) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(file)) {
            generator.accept(writer);
        }
    }
}
