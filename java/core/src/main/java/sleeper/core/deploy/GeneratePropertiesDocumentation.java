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
package sleeper.core.deploy;

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

    private static void generateDocumentation(Path root) throws IOException {
        Path headPath = Files.createDirectories(root.resolve("docs/usage/properties"));

        //---------- Instance Properties ----------
        Path instancePath = Files.createDirectories(headPath.resolve("instance/"));
        //  Athena
        writeFile(instancePath.resolve("athena.md"),
                writer -> writeInstancePropertiesMarkdownTable(InstancePropertyGroup.ATHENA, writer));
        //  Bulk Export
        writeFile(instancePath.resolve("bulk-export.md"),
                writer -> writeInstancePropertiesMarkdownTable(InstancePropertyGroup.BULK_EXPORT, writer));
        //  Common
        writeFile(instancePath.resolve("bulk-import.md"),
                writer -> writeInstancePropertiesMarkdownTable(InstancePropertyGroup.BULK_IMPORT, writer));
        //  Common
        writeFile(instancePath.resolve("common.md"),
                writer -> writeInstancePropertiesMarkdownTable(InstancePropertyGroup.COMMON, writer));
        //  Compaction
        writeFile(instancePath.resolve("compaction.md"),
                writer -> writeInstancePropertiesMarkdownTable(InstancePropertyGroup.COMPACTION, writer));
        //  Garbage Collector
        writeFile(instancePath.resolve("garbage.md"),
                writer -> writeInstancePropertiesMarkdownTable(InstancePropertyGroup.GARBAGE_COLLECTOR, writer));
        //  Ingest
        writeFile(instancePath.resolve("ingest.md"),
                writer -> writeInstancePropertiesMarkdownTable(InstancePropertyGroup.INGEST, writer));
        //  Logging
        writeFile(instancePath.resolve("logging.md"),
                writer -> writeInstancePropertiesMarkdownTable(InstancePropertyGroup.LOGGING, writer));
        //  Metics
        writeFile(instancePath.resolve("metrics.md"),
                writer -> writeInstancePropertiesMarkdownTable(InstancePropertyGroup.METRICS, writer));
        //  Parition Splitting
        writeFile(instancePath.resolve("partition.md"),
                writer -> writeInstancePropertiesMarkdownTable(InstancePropertyGroup.PARTITION_SPLITTING, writer));
        //  Query
        writeFile(instancePath.resolve("query.md"),
                writer -> writeInstancePropertiesMarkdownTable(InstancePropertyGroup.QUERY, writer));

        //---------- Table Properties ----------
        Path tablePath = Files.createDirectories(headPath.resolve("table/"));

        // Bulk Import
        writeFile(tablePath.resolve("bulk-import.md"),
                writer -> writeTablePropertiesMarkdownTable(TablePropertyGroup.BULK_IMPORT, writer));

        // Compaction
        writeFile(tablePath.resolve("compaction.md"),
                writer -> writeTablePropertiesMarkdownTable(TablePropertyGroup.COMPACTION, writer));

        // Data Definition
        writeFile(tablePath.resolve("data-definition.md"),
                writer -> writeTablePropertiesMarkdownTable(TablePropertyGroup.DATA_DEFINITION, writer));

        // Data storage
        writeFile(tablePath.resolve("data-storage.md"),
                writer -> writeTablePropertiesMarkdownTable(TablePropertyGroup.DATA_STORAGE, writer));

        // Ingest
        writeFile(tablePath.resolve("ingest.md"),
                writer -> writeTablePropertiesMarkdownTable(TablePropertyGroup.INGEST, writer));

        // Metadata
        writeFile(tablePath.resolve("metadata.md"),
                writer -> writeTablePropertiesMarkdownTable(TablePropertyGroup.METADATA, writer));

        // Parition Splitting
        writeFile(tablePath.resolve("parition-splitting.md"),
                writer -> writeTablePropertiesMarkdownTable(TablePropertyGroup.PARTITION_SPLITTING, writer));

        // Query Execution
        writeFile(tablePath.resolve("query-execution.md"),
                writer -> writeTablePropertiesMarkdownTable(TablePropertyGroup.QUERY_EXECUTION, writer));
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
