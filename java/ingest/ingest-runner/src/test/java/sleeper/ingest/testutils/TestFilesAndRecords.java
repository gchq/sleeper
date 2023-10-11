/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.ingest.testutils;

import org.apache.hadoop.conf.Configuration;

import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.ingest.testutils.ResultVerifier.readRecordsFromPartitionDataFile;

public class TestFilesAndRecords {

    private final List<FileInfo> files;
    private final Map<String, List<Record>> recordsByFilename;

    private TestFilesAndRecords(List<FileInfo> files, Map<String, List<Record>> recordsByFilename) {
        this.files = files;
        this.recordsByFilename = recordsByFilename;
    }

    public static TestFilesAndRecords loadActiveFiles(
            StateStore stateStore, Schema schema, Configuration configuration) {
        try {
            List<FileInfo> files = stateStore.getActiveFiles();
            Map<String, List<Record>> recordsByFilename = files.stream()
                    .map(file -> Map.entry(file.getFilename(), readRecordsFromPartitionDataFile(schema, file, configuration)))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            return new TestFilesAndRecords(files, recordsByFilename);
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }

    public List<FileInfo> getFiles() {
        return files;
    }

    public Stream<Record> streamAllRecords() {
        return recordsByFilename.values().stream().flatMap(List::stream);
    }

    public Stream<FileInfo> streamFilesWithPartitionId(String partitionId) {
        return files.stream().filter(file -> Objects.equals(partitionId, file.getPartitionId()));
    }

    public List<Record> getRecordsInFile(FileInfo file) {
        return recordsByFilename.get(file.getFilename());
    }
}
