/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.systemtest.suite;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.properties.model.OptionalStack;
import sleeper.core.row.Row;
import sleeper.query.core.output.ResultsOutput;
import sleeper.query.runner.output.S3ResultsOutput;
import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.suite.testutil.SystemTest;
import sleeper.systemtest.suite.testutil.parallel.Slow1;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.REENABLE_OPTIONAL_STACKS;

@SystemTest
// Slow because it needs to do multiple CDK deployments
@Slow1
public class AutoDeleteS3ObjectsST {

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp(SleeperDsl sleeper) {
        sleeper.connectToInstanceAddOnlineTable(REENABLE_OPTIONAL_STACKS);
    }

    @AfterEach
    void tearDown(SleeperDsl sleeper) {
        sleeper.disableOptionalStacks(OptionalStack.all());
    }

    @Test
    void shouldRemoveQueryStackWithDataInQueryBucket(SleeperDsl sleeper) {

        // Given the Query Stack is enabled
        sleeper.enableOptionalStacks(List.of(OptionalStack.QueryStack));

        // And rows have been ingested into the table
        sleeper.ingest().direct(tempDir).numberedRows(LongStream.range(0, 100));

        // When a query is executed and results are written to the query results bucket
        List<Row> queryResults = sleeper.query().byQueue().allRowsWithProcessingConfig(config -> config
                .resultsPublisherConfig(Map.of(ResultsOutput.DESTINATION, S3ResultsOutput.S3)));

        // And the Query Stack is removed
        sleeper.disableOptionalStacks(List.of(OptionalStack.QueryStack));

        // Then the Query Stack can be removed with query results present in the bucket
        assertThat(queryResults).containsExactlyElementsOf(
                sleeper.generateNumberedRows().iterableOverRange(0, 100));
    }
}
