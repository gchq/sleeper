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

package sleeper.clients.report.ingest.batcher;

import sleeper.clients.util.tablewriter.TableField;
import sleeper.clients.util.tablewriter.TableRow;
import sleeper.clients.util.tablewriter.TableWriterFactory;
import sleeper.core.table.TableStatus;
import sleeper.core.table.TableStatusProvider;
import sleeper.ingest.batcher.core.IngestBatcherTrackedFile;

import java.io.PrintStream;
import java.util.List;

import static sleeper.core.util.NumberFormatUtils.formatBytesAsHumanReadableString;

/**
 * Creates reports in human-readable string format on the status of files tracked by the ingest batcher. This produces
 * a table.
 */
public class StandardIngestBatcherReporter implements IngestBatcherReporter {
    private final TableField fileNameField;
    private final TableField fileSizeBytesField;
    private final TableField tableNameField;
    private final TableField receivedTimeField;
    private final TableField stateField;
    private final TableField jobIdField;
    private final TableWriterFactory tableFactory;
    private final PrintStream out;

    public StandardIngestBatcherReporter() {
        this(System.out);
    }

    public StandardIngestBatcherReporter(PrintStream out) {
        TableWriterFactory.Builder tableFactoryBuilder = TableWriterFactory.builder();
        stateField = tableFactoryBuilder.addField("STATE");
        fileNameField = tableFactoryBuilder.addField("FILENAME");
        fileSizeBytesField = tableFactoryBuilder.addField("FILESIZE");
        tableNameField = tableFactoryBuilder.addField("TABLE_NAME");
        receivedTimeField = tableFactoryBuilder.addField("RECEIVED_TIME");
        jobIdField = tableFactoryBuilder.addField("JOB_ID");
        tableFactory = tableFactoryBuilder.build();
        this.out = out;
    }

    @Override
    public void report(List<IngestBatcherTrackedFile> statusList, BatcherQuery.Type queryType, TableStatusProvider tableProvider) {
        out.println();
        out.println("Ingest Batcher Report");
        out.println("---------------------");
        printSummary(statusList, queryType);
        tableFactory.tableBuilder()
                .itemsAndWriter(statusList, (item, builder) -> writeFileRequest(item, builder, tableProvider))
                .showField(queryType == BatcherQuery.Type.ALL, jobIdField)
                .build().write(out);
    }

    private void printSummary(List<IngestBatcherTrackedFile> statusList, BatcherQuery.Type queryType) {
        long batchedFiles = statusList.stream().filter(IngestBatcherTrackedFile::isAssignedToJob).count();
        out.println("Total pending files: " + (statusList.size() - batchedFiles));
        if (queryType == BatcherQuery.Type.ALL) {
            out.println("Total batched files: " + batchedFiles);
        }
    }

    private void writeFileRequest(IngestBatcherTrackedFile fileIngestRequest, TableRow.Builder builder, TableStatusProvider tableProvider) {
        builder
                .value(stateField, fileIngestRequest.isAssignedToJob() ? "BATCHED" : "PENDING")
                .value(fileNameField, fileIngestRequest.getFile())
                .value(fileSizeBytesField, formatBytesAsHumanReadableString(fileIngestRequest.getFileSizeBytes()))
                .value(tableNameField, tableProvider.getById(fileIngestRequest.getTableId())
                        .map(TableStatus::getTableName)
                        .orElseGet(() -> "<deleted table: " + fileIngestRequest.getTableId() + ">"))
                .value(receivedTimeField, fileIngestRequest.getReceivedTime())
                .value(jobIdField, fileIngestRequest.getJobId());
    }
}
