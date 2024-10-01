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

package sleeper.systemtest.dsl.sourcedata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.systemtest.dsl.instance.DeployedSystemTestResources;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

public class IngestSourceFilesContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(IngestSourceFilesContext.class);

    private final SystemTestInstanceContext instance;
    private final Map<String, String> filenameToPath = new TreeMap<>();
    private final String sourceBucketFolderName = UUID.randomUUID().toString();
    private Supplier<String> bucketName;
    private Supplier<String> testFolderName;

    public IngestSourceFilesContext(DeployedSystemTestResources systemTest, SystemTestInstanceContext instance) {
        this.instance = instance;
        bucketName = systemTest::getSystemTestBucketName;
        testFolderName = () -> sourceBucketFolderName;
    }

    public void useDataBucket() {
        bucketName = () -> instance.getInstanceProperties().get(DATA_BUCKET);
        testFolderName = () -> instance.getTableProperties().get(TABLE_ID);
    }

    public void writeFile(IngestSourceFilesDriver driver, String filename, boolean writeSketches, Stream<Record> records) {
        writeFile(driver, instance.getInstanceProperties(), instance.getTableProperties(), filename, writeSketches, records);
    }

    public void writeFile(IngestSourceFilesDriver driver, Schema schema, String filename, boolean writeSketches, Stream<Record> records) {
        InstanceProperties instanceProperties = instance.getInstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        writeFile(driver, instanceProperties, tableProperties, filename, writeSketches, records);
    }

    private void writeFile(
            IngestSourceFilesDriver driver, InstanceProperties instanceProperties, TableProperties tableProperties,
            String filename, boolean writeSketches, Stream<Record> records) {
        String path = instance.getInstanceProperties().get(FILE_SYSTEM) + generateFilePathNoFs(filename);
        driver.writeFile(instanceProperties, tableProperties, path, writeSketches, records.iterator());
        filenameToPath.put(filename, path);
        LOGGER.info("Wrote source file {}, path: {}", filename, path);
    }

    public String getFilePath(String name) {
        String path = filenameToPath.get(name);
        if (path == null) {
            throw new IllegalStateException("Source file does not exist: " + name);
        }
        return path;
    }

    public List<String> getIngestJobFilesInBucket(Stream<String> files) {
        return files.map(this::ingestJobFileInBucket)
                .collect(Collectors.toUnmodifiableList());
    }

    public String ingestJobFileInBucket(String filename) {
        return generateFilePathNoFs(filename);
    }

    private String generateFilePathNoFs(String filename) {
        return bucketName.get() + "/" + testFolderName.get() + "/" + filename;
    }

    public String getSourceBucketName() {
        return bucketName.get();
    }
}
