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

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.query.core.recordretrieval.InMemoryDataStore;
import sleeper.sketches.Sketches;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesDriver;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;

public class InMemorySourceFilesDriver implements IngestSourceFilesDriver {

    private final InMemoryDataStore sourceFiles;
    private final InMemoryDataStore dataStore;
    private final InMemorySketchesStore sketchesStore;

    public InMemorySourceFilesDriver(
            InMemoryDataStore sourceFiles, InMemoryDataStore dataStore, InMemorySketchesStore sketchesStore) {
        this.sourceFiles = sourceFiles;
        this.dataStore = dataStore;
        this.sketchesStore = sketchesStore;
    }

    @Override
    public void writeFile(InstanceProperties instanceProperties, TableProperties tableProperties,
            String path, boolean writeSketches, Iterator<Record> records) {
        List<Record> recordList = new ArrayList<>();
        Schema schema = tableProperties.getSchema();
        Sketches sketches = Sketches.from(schema);
        for (Record record : (Iterable<Record>) () -> records) {
            recordList.add(record);
            sketches.update(schema, record);
        }
        if (path.contains(instanceProperties.get(DATA_BUCKET))) {
            dataStore.addFile(path, recordList);
        } else {
            sourceFiles.addFile(path, recordList);
        }
        if (writeSketches) {
            sketchesStore.addSketchForFile(path, sketches);
        }
    }
}
