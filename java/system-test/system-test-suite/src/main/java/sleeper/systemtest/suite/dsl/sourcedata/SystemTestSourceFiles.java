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

package sleeper.systemtest.suite.dsl.sourcedata;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.systemtest.drivers.sourcedata.AwsIngestSourceFilesDriver;
import sleeper.systemtest.drivers.sourcedata.IngestSourceFilesContext;
import sleeper.systemtest.dsl.instance.SleeperInstanceContext;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesDriver;

import java.util.stream.LongStream;
import java.util.stream.Stream;

public class SystemTestSourceFiles {
    private final SleeperInstanceContext instance;
    private final IngestSourceFilesContext context;
    private final IngestSourceFilesDriver driver;
    private boolean writeSketches = false;

    public SystemTestSourceFiles(SleeperInstanceContext instance, IngestSourceFilesContext context) {
        this.instance = instance;
        this.context = context;
        this.driver = new AwsIngestSourceFilesDriver(context);
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
        driver.writeFile(
                instance.getInstanceProperties(), instance.getTableProperties(),
                filename, writeSketches, records.iterator());
        return this;
    }

    private SystemTestSourceFiles create(Schema schema, String filename, Stream<Record> records) {
        InstanceProperties instanceProperties = instance.getInstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        driver.writeFile(instanceProperties, tableProperties, filename, writeSketches, records.iterator());
        return this;
    }
}
