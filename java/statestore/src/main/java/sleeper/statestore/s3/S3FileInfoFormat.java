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

package sleeper.statestore.s3;

import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileReferenceCount;

public class S3FileInfoFormat {
    private S3FileInfoFormat() {
    }

    public static Schema createFileInfoSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("fileName", new StringType()))
                .valueFields(
                        new Field("fileStatus", new StringType()),
                        new Field("partitionId", new StringType()),
                        new Field("lastStateStoreUpdateTime", new LongType()),
                        new Field("numberOfRecords", new LongType()),
                        new Field("jobId", new StringType()),
                        new Field("countApproximate", new StringType()),
                        new Field("onlyContainsDataForThisPartition", new StringType()))
                .build();
    }

    public static Schema createFileReferenceCountSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("fileName", new StringType()))
                .valueFields(
                        new Field("lastStateStoreUpdateTime", new LongType()),
                        new Field("numberOfReferences", new LongType()))
                .build();
    }

    public static Record getRecordFromFileInfo(FileInfo fileInfo) {
        Record record = new Record();
        record.put("fileName", fileInfo.getFilename());
        record.put("fileStatus", "" + fileInfo.getFileStatus());
        record.put("partitionId", fileInfo.getPartitionId());
        record.put("lastStateStoreUpdateTime", fileInfo.getLastStateStoreUpdateTime());
        record.put("numberOfRecords", fileInfo.getNumberOfRecords());
        if (null == fileInfo.getJobId()) {
            record.put("jobId", "null");
        } else {
            record.put("jobId", fileInfo.getJobId());
        }
        record.put("countApproximate", String.valueOf(fileInfo.isCountApproximate()));
        record.put("onlyContainsDataForThisPartition", String.valueOf(fileInfo.onlyContainsDataForThisPartition()));
        return record;
    }

    public static Record getRecordFromFileReferenceCount(FileReferenceCount fileReferenceCount) {
        Record record = new Record();
        record.put("fileName", fileReferenceCount.getFilename());
        record.put("lastStateStoreUpdateTime", fileReferenceCount.getLastUpdateTime());
        record.put("numberOfReferences", fileReferenceCount.getNumberOfReferences());
        return record;
    }

    public static FileInfo getFileInfoFromRecord(Record record) {
        String jobId = (String) record.get("jobId");
        return FileInfo.wholeFile()
                .filename((String) record.get("fileName"))
                .fileStatus(FileInfo.FileStatus.valueOf((String) record.get("fileStatus")))
                .partitionId((String) record.get("partitionId"))
                .lastStateStoreUpdateTime((Long) record.get("lastStateStoreUpdateTime"))
                .numberOfRecords((Long) record.get("numberOfRecords"))
                .jobId("null".equals(jobId) ? null : jobId)
                .countApproximate(record.get("countApproximate").equals("true"))
                .onlyContainsDataForThisPartition(record.get("onlyContainsDataForThisPartition").equals("true"))
                .build();
    }

    public static FileReferenceCount getFileReferenceCountFromRecord(Record record) {
        return FileReferenceCount.builder()
                .filename((String) record.get("fileName"))
                .numberOfReferences((Long) record.get("numberOfReferences"))
                .lastUpdateTime((Long) record.get("lastStateStoreUpdateTime"))
                .build();
    }
}
