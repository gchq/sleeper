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
package sleeper.compaction.status.job;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.status.DynamoDBRecordBuilder;

import java.time.Instant;
import java.util.Map;

public class DynamoDBCompactionJobStatusFormat {

    private DynamoDBCompactionJobStatusFormat() {
    }

    public static final String JOB_ID = "JobId";
    public static final String UPDATE_TIME = "UpdateTime";
    public static final String UPDATE_TYPE = "UpdateType";
    public static final String PARTITION_ID = "PartitionId";
    public static final String INPUT_FILES_COUNT = "InputFilesCount";
    public static final String SPLIT_TO_PARTITION_IDS = "SplitToPartitionIds";
    public static final String START_TIME = "StartTime";

    public static final String UPDATE_TYPE_CREATED = "created";
    public static final String UPDATE_TYPE_STARTED = "started";

    public static Map<String, AttributeValue> createJobCreatedRecord(CompactionJob job) {
        return createJobRecord(job, UPDATE_TYPE_CREATED)
                .string(PARTITION_ID, job.getPartitionId())
                .number(INPUT_FILES_COUNT, job.getInputFiles().size())
                .apply(builder -> {
                    if (job.isSplittingJob()) {
                        builder.string(SPLIT_TO_PARTITION_IDS, String.join(", ", job.getChildPartitions()));
                    }
                }).build();
    }

    public static Map<String, AttributeValue> createJobStartedRecord(CompactionJob job, Instant startTime) {
        return createJobRecord(job, UPDATE_TYPE_STARTED)
                .number(START_TIME, startTime.toEpochMilli())
                .build();
    }

    private static DynamoDBRecordBuilder createJobRecord(CompactionJob job, String updateType) {
        return new DynamoDBRecordBuilder()
                .string(JOB_ID, job.getId())
                .number(UPDATE_TIME, Instant.now().toEpochMilli())
                .string(UPDATE_TYPE, updateType);
    }

}
