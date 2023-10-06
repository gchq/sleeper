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
package sleeper.compaction.store;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.key.Key;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import java.util.ArrayList;
import java.util.List;

public class StoreUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(StoreUtils.class);

    private StoreUtils() {
    }

    public static boolean updateStateStoreSuccess(List<String> inputFiles,
                                                   String outputFile,
                                                   String partitionId,
                                                   long recordsWritten,
                                                   StateStore stateStore,
                                                   List<PrimitiveType> rowKeyTypes) {
        List<FileInfo> filesToBeMarkedReadyForGC = new ArrayList<>();
        for (String file : inputFiles) {
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(rowKeyTypes)
                    .filename(file)
                    .partitionId(partitionId)
                    .fileStatus(FileInfo.FileStatus.ACTIVE)
                    .build();
            filesToBeMarkedReadyForGC.add(fileInfo);
        }
        FileInfo fileInfo = FileInfo.builder()
                .rowKeyTypes(rowKeyTypes)
                .filename(outputFile)
                .partitionId(partitionId)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .numberOfRecords(recordsWritten)
                .build();
        try {
            stateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(filesToBeMarkedReadyForGC, fileInfo);
            LOGGER.debug("Called atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile method on DynamoDBStateStore");
            return true;
        } catch (StateStoreException e) {
            LOGGER.error("Exception updating DynamoDB (moving input files to ready for GC and creating new active file): {}", e.getMessage());
            return false;
        }
    }

    public static boolean updateStateStoreSuccess(List<String> inputFiles,
                                                   Pair<String, String> outputFiles,
                                                   String partition,
                                                   List<String> childPartitions,
                                                   Pair<Long, Long> recordsWritten,
                                                   StateStore stateStore,
                                                   List<PrimitiveType> rowKeyTypes) {
        List<FileInfo> filesToBeMarkedReadyForGC = new ArrayList<>();
        for (String file : inputFiles) {
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(rowKeyTypes)
                    .filename(file)
                    .partitionId(partition)
                    .fileStatus(FileInfo.FileStatus.ACTIVE)
                    .build();
            filesToBeMarkedReadyForGC.add(fileInfo);
        }
        FileInfo leftFileInfo = FileInfo.builder()
                .rowKeyTypes(rowKeyTypes)
                .filename(outputFiles.getLeft())
                .partitionId(childPartitions.get(0))
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .numberOfRecords(recordsWritten.getLeft())
                .build();
        FileInfo rightFileInfo = FileInfo.builder()
                .rowKeyTypes(rowKeyTypes)
                .filename(outputFiles.getRight())
                .partitionId(childPartitions.get(1))
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .numberOfRecords(recordsWritten.getRight())
                .build();
        try {
            stateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(filesToBeMarkedReadyForGC, leftFileInfo, rightFileInfo);
            LOGGER.debug("Called atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile method on DynamoDBStateStore");
            return true;
        } catch (StateStoreException e) {
            LOGGER.error("Exception updating DynamoDB while moving input files to ready for GC and creating new active file", e);
            return false;
        }
    }
}