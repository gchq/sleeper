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
package sleeper.compaction.core.job.commit;

import org.approvaltests.Approvals;
import org.approvaltests.core.Options;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.ReplaceFileReferencesRequest;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class CompactionCommitRequestSerDeTest {

    CompactionCommitRequestSerDe serDe = new CompactionCommitRequestSerDe();

    @Test
    @Disabled
    void shouldSerialiseCompactionCommitRequest() {
        // Given
        PartitionTree partitions = new PartitionsBuilder(schemaWithKey("key")).singlePartition("root").buildTree();
        ReplaceFileReferencesRequest filesRequest = ReplaceFileReferencesRequest.builder()
                .jobId("test-job")
                .taskId("test-task")
                .jobRunId("test-run")
                .inputFiles(List.of("test.parquet"))
                .newReference(FileReferenceFactory.from(partitions).rootFile("output.parquet", 200))
                .build();
        Runnable callbackOnFail = () -> {
        };
        CompactionCommitRequest request = new CompactionCommitRequest("test-table", filesRequest, callbackOnFail);

        // When
        String json = serDe.toJson(request);
        CompactionCommitRequest found = serDe.fromJsonWithCallbackOnFail(json, callbackOnFail);

        // Then
        assertThat(found).isEqualTo(request);
        Approvals.verify(json, new Options().forFile().withExtension(".json"));
    }

}
