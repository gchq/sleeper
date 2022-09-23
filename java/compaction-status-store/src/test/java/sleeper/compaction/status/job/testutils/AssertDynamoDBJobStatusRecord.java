/*
 * Copyright 2022 Crown Copyright
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
package sleeper.compaction.status.job.testutils;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import sleeper.compaction.job.CompactionJobSummary;
import sleeper.compaction.status.DynamoDBRecordBuilder;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.FINISH_TIME;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.INPUT_FILES_COUNT;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.JOB_ID;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.PARTITION_ID;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.RECORDS_READ;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.RECORDS_WRITTEN;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.SPLIT_TO_PARTITION_IDS;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.START_TIME;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.TABLE_NAME;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.UPDATE_TIME;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.UPDATE_TYPE;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.UPDATE_TYPE_CREATED;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.UPDATE_TYPE_FINISHED;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.UPDATE_TYPE_STARTED;
import static sleeper.compaction.status.job.testutils.AssertDynamoDBRecord.keySet;

public class AssertDynamoDBJobStatusRecord {

    private AssertDynamoDBJobStatusRecord() {
    }

    private static final Set<String> CREATE_COMPACTION_KEYS = keySet(
            JOB_ID, UPDATE_TIME, UPDATE_TYPE, TABLE_NAME, PARTITION_ID, INPUT_FILES_COUNT);

    private static final Set<String> CREATE_SPLITTING_KEYS = keySet(
            JOB_ID, UPDATE_TIME, UPDATE_TYPE, TABLE_NAME, PARTITION_ID, INPUT_FILES_COUNT, SPLIT_TO_PARTITION_IDS);

    private static final Set<String> START_COMPACTION_KEYS = keySet(
            JOB_ID, UPDATE_TIME, UPDATE_TYPE, START_TIME);

    private static final Set<String> FINISH_COMPACTION_KEYS = keySet(
            JOB_ID, UPDATE_TIME, UPDATE_TYPE, START_TIME, FINISH_TIME, RECORDS_READ, RECORDS_WRITTEN);

    public static AssertDynamoDBRecord createCompaction(String jobId, int inputFilesCount, String tableName, String partitionId) {
        return AssertDynamoDBRecord.expected(CREATE_COMPACTION_KEYS, new DynamoDBRecordBuilder()
                .string(JOB_ID, jobId)
                .string(UPDATE_TYPE, UPDATE_TYPE_CREATED)
                .string(PARTITION_ID, partitionId)
                .string(TABLE_NAME, tableName)
                .number(INPUT_FILES_COUNT, inputFilesCount));
    }

    public static AssertDynamoDBRecord createSplittingCompaction(
            String jobId, int inputFilesCount, String tableName, String partitionId, String splitToPartitionIds) {
        return AssertDynamoDBRecord.expected(CREATE_SPLITTING_KEYS, new DynamoDBRecordBuilder()
                .string(JOB_ID, jobId)
                .string(UPDATE_TYPE, UPDATE_TYPE_CREATED)
                .string(PARTITION_ID, partitionId)
                .string(TABLE_NAME, tableName)
                .number(INPUT_FILES_COUNT, inputFilesCount)
                .string(SPLIT_TO_PARTITION_IDS, splitToPartitionIds));
    }

    public static AssertDynamoDBRecord startCompaction(String jobId, Instant startTime) {
        return AssertDynamoDBRecord.expected(START_COMPACTION_KEYS, new DynamoDBRecordBuilder()
                .string(JOB_ID, jobId)
                .string(UPDATE_TYPE, UPDATE_TYPE_STARTED)
                .number(START_TIME, startTime.toEpochMilli()));
    }

    public static AssertDynamoDBRecord finishCompaction(String jobId, CompactionJobSummary summary) {
        return AssertDynamoDBRecord.expected(FINISH_COMPACTION_KEYS, new DynamoDBRecordBuilder()
                .string(JOB_ID, jobId)
                .string(UPDATE_TYPE, UPDATE_TYPE_FINISHED)
                .number(START_TIME, summary.getStartTime().toEpochMilli())
                .number(FINISH_TIME, summary.getFinishTime().toEpochMilli())
                .number(RECORDS_READ, summary.getLinesRead())
                .number(RECORDS_WRITTEN, summary.getLinesWritten()));
    }

    public static AssertDynamoDBRecord actualIgnoringUpdateTime(Map<String, AttributeValue> item) {
        return AssertDynamoDBRecord.actualIgnoringKeys(item, keySet(UPDATE_TIME));
    }
}
