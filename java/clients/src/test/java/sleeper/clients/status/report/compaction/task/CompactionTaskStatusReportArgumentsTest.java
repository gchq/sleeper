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
package sleeper.clients.status.report.compaction.task;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CompactionTaskStatusReportArgumentsTest {

    @Test
    public void shouldDefaultToQueryingAllTasksWithStandardOutput() {
        // Given / When
        CompactionTaskStatusReportArguments arguments = CompactionTaskStatusReportArguments.fromArgs("test-instance");

        // Then
        assertThat(arguments.getInstanceId()).isEqualTo("test-instance");
        assertThat(arguments.getReporter()).isInstanceOf(StandardCompactionTaskStatusReporter.class);
        assertThat(arguments.getQuery()).isSameAs(CompactionTaskQuery.ALL);
    }

    @Test
    public void shouldSetJsonReporterWhenArgumentProvided() {
        // Given / When
        CompactionTaskStatusReportArguments arguments = CompactionTaskStatusReportArguments.fromArgs("test-instance", "json");

        // Then
        assertThat(arguments.getReporter()).isInstanceOf(JsonCompactionTaskStatusReporter.class);
    }

    @Test
    public void shouldSetUnfinishedQueryTypeWhenArgumentProvided() {
        // Given / When
        CompactionTaskStatusReportArguments arguments = CompactionTaskStatusReportArguments.fromArgs("test-instance", "standard", "-u");

        // Then
        assertThat(arguments.getQuery()).isSameAs(CompactionTaskQuery.UNFINISHED);
    }

    @Test
    public void shouldFailWhenNoInstanceIdSpecified() {
        // When / Then
        assertThatThrownBy(CompactionTaskStatusReportArguments::fromArgs)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Wrong number of arguments");
    }

    @Test
    public void shouldFailWhenTooManyArgumentsSpecified() {
        // When / Then
        assertThatThrownBy(() -> CompactionTaskStatusReportArguments.fromArgs(
                "test-instance", "standard", "-a", "extra-argument"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Wrong number of arguments");
    }
}
