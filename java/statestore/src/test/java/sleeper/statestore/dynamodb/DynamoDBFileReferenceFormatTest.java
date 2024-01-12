/*
 * Copyright 2022-2024 Crown Copyright
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

package sleeper.statestore.dynamodb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.junit.jupiter.api.Test;

import sleeper.core.statestore.FileReference;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class DynamoDBFileReferenceFormatTest {
    private final String tableId = "test-table-id";
    private final DynamoDBFileReferenceFormat fileInfoFormat = new DynamoDBFileReferenceFormat(tableId);

    @Test
    void shouldCreateActiveFileRecord() {
        // Given
        FileReference fileReference = createActiveFile("file1.parquet", "partition1", 100);

        // When / Then
        assertThat(fileInfoFormat.createActiveFileRecord(fileReference))
                .isEqualTo(Map.of(
                        "PartitionIdAndFileName", new AttributeValue().withS("partition1|file1.parquet"),
                        "TableId", new AttributeValue().withS("test-table-id"),
                        "NumRecords", new AttributeValue().withN("100"),
                        "IsCountApproximate", new AttributeValue().withBOOL(false),
                        "OnlyContainsDataForThisPartition", new AttributeValue().withBOOL(true)
                ));
    }

    @Test
    void shouldCreateHashAndSortKeyForActiveFile() {
        // Given
        FileReference fileReference = createActiveFile("file1.parquet", "partition1", 100);

        // When / Then
        assertThat(fileInfoFormat.createActiveFileKey(fileReference))
                .isEqualTo(Map.of(
                        "TableId", new AttributeValue().withS("test-table-id"),
                        "PartitionIdAndFileName", new AttributeValue().withS("partition1|file1.parquet")
                ));
    }

    @Test
    void shouldCreateFileInfoFromActiveFileRecord() {
        // Given
        Map<String, AttributeValue> item = Map.of(
                "PartitionIdAndFileName", new AttributeValue().withS("partition1|file1.parquet"),
                "TableId", new AttributeValue().withS("test-table-id"),
                "NumRecords", new AttributeValue().withN("100"),
                "IsCountApproximate", new AttributeValue().withBOOL(true),
                "OnlyContainsDataForThisPartition", new AttributeValue().withBOOL(false)
        );

        // When / Then
        assertThat(fileInfoFormat.getFileInfoFromAttributeValues(item))
                .isEqualTo(FileReference.partialFile()
                        .filename("file1.parquet")
                        .partitionId("partition1")
                        .numberOfRecords(100L)
                        .build());
    }

    private FileReference createActiveFile(String fileName, String partitionId, long numberOfRecords) {
        return FileReference.wholeFile()
                .filename(fileName)
                .partitionId(partitionId)
                .numberOfRecords(numberOfRecords)
                .build();
    }
}
