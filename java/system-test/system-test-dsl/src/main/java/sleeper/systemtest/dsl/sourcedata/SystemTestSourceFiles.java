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

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.util.stream.LongStream;
import java.util.stream.Stream;

public class SystemTestSourceFiles {
    private static final Logger LOGGER = LoggerFactory.getLogger(SystemTestSourceFiles.class);

    private final SystemTestInstanceContext instance;
    private final IngestSourceFilesContext context;
    private final IngestSourceFilesDriver driver;
    private boolean writeSketches = false;

    public SystemTestSourceFiles(SystemTestInstanceContext instance,
                                 IngestSourceFilesContext context,
                                 IngestSourceFilesDriver driver) {
        this.instance = instance;
        this.context = context;
        this.driver = driver;
    }

    public SystemTestSourceFiles inDataBucket() {
        context.useDataBucket();
        return this;
    }

    public SystemTestSourceFiles writeSketches() {
        writeSketches = true;
        return this;
    }

    public SystemTestSourceFiles createWithNumberedRecords(String filename, LongStream numbers) {
        return create(filename, instance.generateNumberedRecords(numbers));
    }

    public SystemTestSourceFiles createWithNumberedRecords(Schema schema, String filename, LongStream numbers) {
        return create(schema, filename, instance.generateNumberedRecords(schema, numbers));
    }

    public SystemTestSourceFiles create(String filename, Record... records) {
        return create(filename, Stream.of(records));
    }

    private SystemTestSourceFiles create(String filename, Stream<Record> records) {
        writeFile(instance.getInstanceProperties(), instance.getTableProperties(), filename, records);
        return this;
    }

    private SystemTestSourceFiles create(Schema schema, String filename, Stream<Record> records) {
        InstanceProperties instanceProperties = instance.getInstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        writeFile(instanceProperties, tableProperties, filename, records);
        return this;
    }

    private void writeFile(InstanceProperties instanceProperties, TableProperties tableProperties,
                           String filename, Stream<Record> records) {
        String path = context.generateFilePath(filename);
        driver.writeFile(instanceProperties, tableProperties, path, writeSketches, records.iterator());
        context.wroteFile(filename, path);
        LOGGER.info("Wrote source file {}, path: {}", filename, path);
    }
}
