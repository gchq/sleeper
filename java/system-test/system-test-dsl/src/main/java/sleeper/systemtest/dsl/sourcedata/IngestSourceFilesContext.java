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

package sleeper.systemtest.dsl.sourcedata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.systemtest.dsl.instance.DeployedSystemTestResources;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;

public class IngestSourceFilesContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(IngestSourceFilesContext.class);

    private final SystemTestInstanceContext instance;
    private final Map<String, String> filenameToPath = new TreeMap<>();
    final String sourceBucketFolderName = UUID.randomUUID().toString();
    private SourceFilesFolder lastFolder;

    public IngestSourceFilesContext(DeployedSystemTestResources systemTest, SystemTestInstanceContext instance) {
        this.instance = instance;
    }

    public void writeFile(IngestSourceFilesDriver driver, String filename, SourceFilesFolder folder, boolean writeSketches, Iterator<Row> rows) {
        writeFile(driver, instance.getInstanceProperties(), instance.getTableProperties(), filename, folder, writeSketches, rows);
    }

    public void writeFile(IngestSourceFilesDriver driver, Schema schema, String filename, SourceFilesFolder folder, boolean writeSketches, Iterator<Row> rows) {
        InstanceProperties instanceProperties = instance.getInstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        writeFile(driver, instanceProperties, tableProperties, filename, folder, writeSketches, rows);
    }

    private void writeFile(
            IngestSourceFilesDriver driver, InstanceProperties instanceProperties, TableProperties tableProperties,
            String filename, SourceFilesFolder folder, boolean writeSketches, Iterator<Row> rows) {
        String path = instance.getInstanceProperties().get(FILE_SYSTEM) + folder.generateFilePathNoFs(filename);
        driver.writeFile(instanceProperties, tableProperties, path, writeSketches, rows);
        filenameToPath.put(filename, path);
        lastFolder = folder;
        LOGGER.info("Wrote source file {}, path: {}", filename, path);
    }

    public String getFilePath(String name) {
        String path = filenameToPath.get(name);
        if (path == null) {
            throw new IllegalStateException("Source file does not exist: " + name);
        }
        return path;
    }

    public SourceFilesFolder lastFolderWrittenTo() {
        return lastFolder;
    }
}
