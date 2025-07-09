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

package sleeper.systemtest.dsl.testutil.drivers;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.Row;
import sleeper.core.record.testutils.InMemoryRecordStore;
import sleeper.sketches.Sketches;
import sleeper.sketches.testutils.InMemorySketchesStore;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesDriver;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;

public class InMemorySourceFilesDriver implements IngestSourceFilesDriver {

    private final InMemoryRecordStore sourceFiles;
    private final InMemoryRecordStore dataStore;
    private final InMemorySketchesStore sketchesStore;

    public InMemorySourceFilesDriver(
            InMemoryRecordStore sourceFiles, InMemoryRecordStore dataStore, InMemorySketchesStore sketchesStore) {
        this.sourceFiles = sourceFiles;
        this.dataStore = dataStore;
        this.sketchesStore = sketchesStore;
    }

    @Override
    public void writeFile(InstanceProperties instanceProperties, TableProperties tableProperties,
            String path, boolean writeSketches, Iterator<Row> records) {
        List<Row> recordList = new ArrayList<>();
        Sketches sketches = Sketches.from(tableProperties.getSchema());
        for (Row record : (Iterable<Row>) () -> records) {
            recordList.add(record);
            sketches.update(record);
        }
        if (path.contains(instanceProperties.get(DATA_BUCKET))) {
            dataStore.addFile(path, recordList);
        } else {
            sourceFiles.addFile(path, recordList);
        }
        if (writeSketches) {
            sketchesStore.saveFileSketches(path, sketches);
        }
    }
}
