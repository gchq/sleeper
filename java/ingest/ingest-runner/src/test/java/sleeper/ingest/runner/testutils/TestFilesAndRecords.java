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

package sleeper.ingest.runner.testutils;

import org.apache.hadoop.conf.Configuration;

import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.ingest.runner.testutils.ResultVerifier.readRowsFromPartitionDataFile;

public class TestFilesAndRecords {

    private final List<FileReference> files;
    private final Map<String, List<Row>> rowsByFilename;

    private TestFilesAndRecords(List<FileReference> files, Map<String, List<Row>> rowsByFilename) {
        this.files = files;
        this.rowsByFilename = rowsByFilename;
    }

    public static TestFilesAndRecords loadActiveFiles(
            StateStore stateStore, Schema schema, Configuration configuration) {
        List<FileReference> fileReferences = stateStore.getFileReferences();
        Map<String, List<Row>> rowsByFilename = fileReferences.stream()
                .map(file -> Map.entry(file.getFilename(), readRowsFromPartitionDataFile(schema, file, configuration)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return new TestFilesAndRecords(fileReferences, rowsByFilename);
    }

    public List<FileReference> getFiles() {
        return files;
    }

    public Stream<Row> streamAllRecords() {
        return rowsByFilename.values().stream().flatMap(List::stream);
    }

    public Set<Row> getSetOfAllRecords() {
        return streamAllRecords().collect(Collectors.toSet());
    }

    public List<Row> getRecordsInFile(FileReference file) {
        return rowsByFilename.get(file.getFilename());
    }

    public int getNumRecords() {
        return rowsByFilename.values().stream().mapToInt(List::size).sum();
    }

    public TestFilesAndRecords getPartitionData(String partitionId) {
        List<FileReference> partitionFiles = files.stream()
                .filter(file -> Objects.equals(partitionId, file.getPartitionId()))
                .collect(Collectors.toUnmodifiableList());
        Map<String, List<Row>> partitionRecords = partitionFiles.stream()
                .map(file -> Map.entry(file.getFilename(), rowsByFilename.get(file.getFilename())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return new TestFilesAndRecords(partitionFiles, partitionRecords);
    }
}
