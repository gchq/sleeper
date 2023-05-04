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

package sleeper.systemtest.output;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static sleeper.systemtest.output.NightlyTestOutputTestHelper.outputWithStatusCodeByTest;

public class NightlyTestSummaryTableTest {
    @Test
    @Disabled("TODO")
    void shouldAddTestExecutionToEmptySummary() {
        // Given
        NightlyTestSummaryTable summaries = NightlyTestSummaryTable.fromSingleSummary(
                outputWithStatusCodeByTest(Map.of("bulkImportPerformance", 1)));

        // When
        assertThatJson(summaries.toJson())
                .isEqualTo("{\"executions\":[{" +
                        "\"timestamp\":\"2023-05-03T15:15:00Z\"," +
                        "\"tests\": [{\"name\":\"bulkImportPerformance\", \"exitCode\":0}]" +
                        "}]}");
    }
}
