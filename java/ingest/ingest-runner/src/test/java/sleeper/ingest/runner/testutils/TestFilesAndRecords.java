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

package sleeper.ingest.runner.testutils;

import org.apache.hadoop.conf.Configuration;

import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.ingest.runner.testutils.ResultVerifier.readRecordsFromPartitionDataFile;

public class TestFilesAndRecords {

    private final List<FileReference> files;
    private final Map<String, List<Record>> recordsByFilename;

    private TestFilesAndRecords(List<FileReference> files, Map<String, List<Record>> recordsByFilename) {
        this.files = files;
        this.recordsByFilename = recordsByFilename;
    }

    public static TestFilesAndRecords loadActiveFiles(
            StateStore stateStore, Schema schema, Configuration configuration) {
        try {
            List<FileReference> fileReferences = stateStore.getFileReferences();
            Map<String, List<Record>> recordsByFilename = fileReferences.stream()
                    .map(file -> Map.entry(file.getFilename(), readRecordsFromPartitionDataFile(schema, file, configuration)))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            return new TestFilesAndRecords(fileReferences, recordsByFilename);
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }

    public List<FileReference> getFiles() {
        return files;
    }

    public Stream<Record> streamAllRecords() {
        return recordsByFilename.values().stream().flatMap(List::stream);
    }

    public Set<Record> getSetOfAllRecords() {
        return streamAllRecords().collect(Collectors.toSet());
    }

    public List<Record> getRecordsInFile(FileReference file) {
        return recordsByFilename.get(file.getFilename());
    }

    public int getNumRecords() {
        return recordsByFilename.values().stream().mapToInt(List::size).sum();
    }

    public TestFilesAndRecords getPartitionData(String partitionId) {
        List<FileReference> partitionFiles = files.stream()
                .filter(file -> Objects.equals(partitionId, file.getPartitionId()))
                .collect(Collectors.toUnmodifiableList());
        Map<String, List<Record>> partitionRecords = partitionFiles.stream()
                .map(file -> Map.entry(file.getFilename(), recordsByFilename.get(file.getFilename())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return new TestFilesAndRecords(partitionFiles, partitionRecords);
    }
}
