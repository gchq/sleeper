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

package sleeper.systemtest.dsl.testutil.drivers;

import org.apache.datasketches.quantiles.ItemsSketch;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.ingest.impl.partitionfilewriter.PartitionFileWriterUtils;
import sleeper.query.runner.recordretrieval.InMemoryDataStore;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesDriver;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;

public class InMemorySourceFilesDriver implements IngestSourceFilesDriver {

    private final InMemoryDataStore sourceFiles;
    private final InMemoryDataStore data;
    private final InMemorySketchesStore sketches;

    public InMemorySourceFilesDriver(
            InMemoryDataStore sourceFiles, InMemoryDataStore data, InMemorySketchesStore sketches) {
        this.sourceFiles = sourceFiles;
        this.data = data;
        this.sketches = sketches;
    }

    @Override
    public void writeFile(InstanceProperties instanceProperties, TableProperties tableProperties,
            String path, boolean writeSketches, Iterator<Record> records) {
        List<Record> recordList = new ArrayList<>();
        Schema schema = tableProperties.getSchema();
        Map<String, ItemsSketch> keyFieldToSketchMap = PartitionFileWriterUtils.createQuantileSketchMap(schema);
        for (Record record : (Iterable<Record>) () -> records) {
            recordList.add(record);
            PartitionFileWriterUtils.updateQuantileSketchMap(schema, keyFieldToSketchMap, record);
        }
        if (path.contains(instanceProperties.get(DATA_BUCKET))) {
            data.addFile(path, recordList);
        } else {
            sourceFiles.addFile(path, recordList);
        }
        if (writeSketches) {
            sketches.addSketchForFile(path, keyFieldToSketchMap);
        }
    }
}
