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

package sleeper.clients.status.report.ingest.batcher;

import sleeper.clients.util.table.TableField;
import sleeper.clients.util.table.TableRow;
import sleeper.clients.util.table.TableWriterFactory;
import sleeper.ingest.batcher.FileIngestRequest;

import java.io.PrintStream;
import java.util.List;

public class StandardIngestBatcherStatusReporter implements IngestBatcherStatusReporter {
    private final TableField fileNameField;
    private final TableField fileSizeBytesField;
    private final TableField tableNameField;
    private final TableField receivedTimeField;
    private final TableField stateField;
    private final TableField jobIdField;
    private final TableWriterFactory tableFactory;
    private final PrintStream out;

    public StandardIngestBatcherStatusReporter(PrintStream out) {
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
    public void report(List<FileIngestRequest> statusList, BatcherQuery.Type queryType) {
        out.println();
        out.println("Ingest Batcher Status Report");
        out.println("----------------------------");
        printSummary(statusList, queryType);
        tableFactory.tableBuilder()
                .itemsAndWriter(statusList, this::writeFileRequest)
                .showField(queryType == BatcherQuery.Type.ALL, jobIdField)
                .build().write(out);
    }

    private void printSummary(List<FileIngestRequest> statusList, BatcherQuery.Type queryType) {
        long batchedFiles = statusList.stream().filter(FileIngestRequest::isAssignedToJob).count();
        out.println("Total pending files: " + (statusList.size() - batchedFiles));
        if (queryType == BatcherQuery.Type.ALL) {
            out.println("Total batched files: " + (batchedFiles));
        }
    }

    private void writeFileRequest(FileIngestRequest fileIngestRequest, TableRow.Builder builder) {
        builder
                .value(stateField, fileIngestRequest.isAssignedToJob() ? "BATCHED" : "PENDING")
                .value(fileNameField, fileIngestRequest.getFile())
                .value(fileSizeBytesField, fileIngestRequest.getFileSizeBytes())
                .value(tableNameField, fileIngestRequest.getTableName())
                .value(receivedTimeField, fileIngestRequest.getReceivedTime())
                .value(jobIdField, fileIngestRequest.getJobId());
    }
}
