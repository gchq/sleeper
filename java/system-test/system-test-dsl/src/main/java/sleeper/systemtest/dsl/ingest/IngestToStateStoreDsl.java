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

package sleeper.systemtest.dsl.ingest;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.row.Row;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.transactionlog.AddTransactionRequest;
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.instance.DataFilesDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesContext;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesDriver;
import sleeper.systemtest.dsl.sourcedata.SourceFilesFolder;
import sleeper.systemtest.dsl.util.DataFileDuplications;

import java.util.List;
import java.util.stream.Collectors;

public class IngestToStateStoreDsl {

    private final SystemTestContext context;
    private final SystemTestInstanceContext instance;
    private final IngestSourceFilesContext ingestSource;

    public IngestToStateStoreDsl(SystemTestContext context) {
        this.context = context;
        this.instance = context.instance();
        this.ingestSource = context.sourceFiles();
    }

    public DataFileDuplications duplicateFilesOnSamePartitions(int times) {
        return duplicateFilesOnSamePartitions(times, instance.getStateStore().getFileReferences());
    }

    public DataFileDuplications duplicateFilesOnSamePartitions(int times, List<FileReference> fileReferences) {
        DataFileDuplications duplications = DataFileDuplications.duplicateByReferences(dataFilesDriver(), times, fileReferences);
        addFiles(duplications.streamNewReferences().toList());
        return duplications;
    }

    public IngestToStateStoreDsl addFileOnPartition(
            String name, String partitionId, Row... rows) {
        ingestSource.writeFile(sourceFilesDriver(), name, SourceFilesFolder.writeToDataBucket(instance), true, List.of(rows).iterator());
        addFiles(List.of(FileReference.builder()
                .filename(ingestSource.getFilePath(name))
                .partitionId(partitionId)
                .countApproximate(false)
                .onlyContainsDataForThisPartition(true)
                .numberOfRows((long) rows.length)
                .build()));
        return this;
    }

    public IngestToStateStoreDsl addFileOnPartition(
            String name, String partitionId, long numberOfRows) {
        String path = ingestSource.getFilePath(name);
        addFiles(List.of(FileReference.builder()
                .filename(path)
                .partitionId(partitionId)
                .countApproximate(false)
                .onlyContainsDataForThisPartition(true)
                .numberOfRows(numberOfRows)
                .build()));
        return this;
    }

    public IngestToStateStoreDsl addFileOnEveryPartition(String name, long numberOfRows) {
        String path = ingestSource.getFilePath(name);
        PartitionTree partitionTree = new PartitionTree(instance.getStateStore().getAllPartitions());
        List<Partition> leafPartitions = partitionTree.getLeafPartitions();
        long rowsPerPartition = numberOfRows / leafPartitions.size();
        addFiles(leafPartitions.stream()
                .map(partition -> FileReference.builder()
                        .filename(path)
                        .partitionId(partition.getId())
                        .countApproximate(true)
                        .onlyContainsDataForThisPartition(false)
                        .numberOfRows(rowsPerPartition)
                        .build())
                .collect(Collectors.toUnmodifiableList()));
        return this;
    }

    private void addFiles(List<FileReference> fileReferences) {
        instance.getStateStore().addTransaction(AddTransactionRequest.withTransaction(
                AddFilesTransaction.fromReferences(fileReferences))
                .build());
    }

    private DataFilesDriver dataFilesDriver() {
        return instance.adminDrivers().dataFiles(context);
    }

    private IngestSourceFilesDriver sourceFilesDriver() {
        return instance.adminDrivers().sourceFiles(context);
    }
}
