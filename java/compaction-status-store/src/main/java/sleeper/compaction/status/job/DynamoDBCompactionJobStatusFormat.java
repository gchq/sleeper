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

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static sleeper.compaction.status.DynamoDBAttributes.createNumberAttribute;
import static sleeper.compaction.status.DynamoDBAttributes.createStringAttribute;

public class DynamoDBCompactionJobStatusFormat {

    private DynamoDBCompactionJobStatusFormat() {
    }

    public static final String JOB_ID = "JobId";
    public static final String UPDATE_TIME = "UpdateTime";
    public static final String UPDATE_TYPE = "UpdateType";
    public static final String PARTITION_ID = "PartitionId";
    public static final String INPUT_FILES_COUNT = "InputFilesCount";
    public static final String SPLIT_TO_PARTITION_IDS = "SplitToPartitionIds";

    public static final String UPDATE_TYPE_CREATED = "created";

    public static Map<String, AttributeValue> createJobCreatedRecord(CompactionJob job) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(JOB_ID, createStringAttribute(job.getId()));
        item.put(UPDATE_TIME, createNumberAttribute(Instant.now().toEpochMilli()));
        item.put(UPDATE_TYPE, createStringAttribute(UPDATE_TYPE_CREATED));
        item.put(PARTITION_ID, createStringAttribute(job.getPartitionId()));
        item.put(INPUT_FILES_COUNT, createNumberAttribute(job.getInputFiles().size()));
        if (job.isSplittingJob()) {
            item.put(SPLIT_TO_PARTITION_IDS, createStringAttribute(String.join(", ", job.getChildPartitions())));
        }
        return item;
    }

}
