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

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.compaction.status.DynamoDBAttributes.getNumberAttribute;
import static sleeper.compaction.status.DynamoDBAttributes.getStringAttribute;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.INPUT_FILES_COUNT;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.JOB_ID;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.PARTITION_ID;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.SPLIT_TO_PARTITION_IDS;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.UPDATE_TIME;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.UPDATE_TYPE;

public class CompactionJobCreatedRecord implements CompactionJobStatusRecord {

    private static final Set<String> COMPACTION_KEYS = Stream
            .of(JOB_ID, UPDATE_TIME, UPDATE_TYPE, PARTITION_ID, INPUT_FILES_COUNT)
            .collect(Collectors.toSet());

    private static final Set<String> SPLITTING_KEYS = Stream
            .of(JOB_ID, UPDATE_TIME, UPDATE_TYPE, PARTITION_ID, INPUT_FILES_COUNT, SPLIT_TO_PARTITION_IDS)
            .collect(Collectors.toSet());

    private final Set<String> keySet;
    private final String jobId;
    private final String inputFilesCount;
    private final String partitionId;
    private final String splitToPartitionIds;

    private CompactionJobCreatedRecord(
            Set<String> keySet, String jobId, String inputFilesCount, String partitionId, String splitToPartitionIds) {
        this.keySet = keySet;
        this.jobId = jobId;
        this.inputFilesCount = inputFilesCount;
        this.partitionId = partitionId;
        this.splitToPartitionIds = splitToPartitionIds;
    }

    public static CompactionJobStatusRecord readFrom(Map<String, AttributeValue> item) {
        return new CompactionJobCreatedRecord(item.keySet(),
                getStringAttribute(item, JOB_ID),
                getNumberAttribute(item, INPUT_FILES_COUNT),
                getStringAttribute(item, PARTITION_ID),
                getStringAttribute(item, SPLIT_TO_PARTITION_IDS));
    }

    public static CompactionJobStatusRecord createCompaction(String jobId, int inputFilesCount, String partitionId) {
        return new CompactionJobCreatedRecord(COMPACTION_KEYS,
                jobId, "" + inputFilesCount, partitionId, null);
    }

    public static CompactionJobStatusRecord createSplittingCompaction(
            String jobId, int inputFilesCount, String partitionId, String splitToPartitionIds) {
        return new CompactionJobCreatedRecord(SPLITTING_KEYS,
                jobId, "" + inputFilesCount, partitionId, splitToPartitionIds);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompactionJobCreatedRecord that = (CompactionJobCreatedRecord) o;
        return Objects.equals(keySet, that.keySet) && Objects.equals(jobId, that.jobId) && Objects.equals(inputFilesCount, that.inputFilesCount) && Objects.equals(partitionId, that.partitionId) && Objects.equals(splitToPartitionIds, that.splitToPartitionIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keySet, jobId, inputFilesCount, partitionId, splitToPartitionIds);
    }
}
