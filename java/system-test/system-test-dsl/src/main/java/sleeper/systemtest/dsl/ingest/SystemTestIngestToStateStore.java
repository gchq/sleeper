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
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.transactionlog.AddTransactionRequest;
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.instance.DataFileDuplication;
import sleeper.systemtest.dsl.instance.DataFilesDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesContext;

import java.util.List;
import java.util.stream.Collectors;

public class SystemTestIngestToStateStore {

    private final SystemTestContext context;
    private final SystemTestInstanceContext instance;
    private final IngestSourceFilesContext ingestSource;

    public SystemTestIngestToStateStore(SystemTestContext context) {
        this.context = context;
        this.instance = context.instance();
        this.ingestSource = context.sourceFiles();
    }

    public SystemTestIngestToStateStore duplicateFilesOnSamePartition(int times, List<FileReference> fileReferences) {
        addFiles(DataFileDuplication.duplicateByReferences(dataFilesDriver(), times, fileReferences));
        return this;
    }

    public SystemTestIngestToStateStore addFileOnPartition(
            String name, String partitionId, long numberOfRows) throws Exception {
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

    public SystemTestIngestToStateStore addFileOnEveryPartition(String name, long numberOfRows) throws Exception {
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
}
