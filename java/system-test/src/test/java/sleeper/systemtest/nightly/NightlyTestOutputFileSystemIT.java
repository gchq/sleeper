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

package sleeper.systemtest.nightly;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.nightly.NightlyTestOutputTestHelper.emptyOutput;

class NightlyTestOutputFileSystemIT {
    @TempDir
    private Path tempDir;

    @Nested
    @DisplayName("Find log files")
    class FindLogFiles {
        @Test
        void shouldFindLogFile() throws Exception {
            // Given
            Files.writeString(tempDir.resolve("bulkImportPerformance.log"), "test");

            // When / Then
            assertThat(NightlyTestOutput.from(tempDir).streamLogFiles())
                    .containsExactly(tempDir.resolve("bulkImportPerformance.log"));
        }

        @Test
        void shouldIgnoreDirectories() throws Exception {
            // Given
            Files.createDirectory(tempDir.resolve("testDir.log"));

            // When / Then
            assertThat(NightlyTestOutput.from(tempDir).streamLogFiles())
                    .isEmpty();
        }
    }

    @Nested
    @DisplayName("Read status files")
    class ReadStatusFiles {

        @Test
        void shouldReadStatusFiles() throws Exception {
            // Given
            Files.writeString(tempDir.resolve("bulkImportPerformance.status"), "0 bulk-import-instance");
            Files.writeString(tempDir.resolve("compactionPerformance.status"), "1 compaction-instance");

            // When / Then
            assertThat(NightlyTestOutput.from(tempDir))
                    .isEqualTo(new NightlyTestOutput(List.of(
                            TestResult.builder().testName("bulkImportPerformance")
                                    .instanceId("bulk-import-instance").exitCode(0).build(),
                            TestResult.builder().testName("compactionPerformance")
                                    .instanceId("compaction-instance").exitCode(1).build())));
        }

        @Test
        void shouldReadStatusFilesWithCodeOnly() throws Exception {
            // Given
            Files.writeString(tempDir.resolve("bulkImportPerformance.status"), "0");
            Files.writeString(tempDir.resolve("compactionPerformance.status"), "1");

            // When / Then
            assertThat(NightlyTestOutput.from(tempDir))
                    .isEqualTo(new NightlyTestOutput(List.of(
                            TestResult.builder().testName("bulkImportPerformance").exitCode(0).build(),
                            TestResult.builder().testName("compactionPerformance").exitCode(1).build())));
        }

        @Test
        void shouldIgnoreDirectories() throws Exception {
            // Given
            Files.createDirectory(tempDir.resolve("testDir.status"));

            // When / Then
            assertThat(NightlyTestOutput.from(tempDir))
                    .isEqualTo(emptyOutput());
        }
    }

    @Test
    void shouldIgnoreFileWithUnrecognisedExtension() throws Exception {
        // Given
        Files.writeString(tempDir.resolve("bulkImportPerformance.test"), "test");

        // When / Then
        assertThat(NightlyTestOutput.from(tempDir))
                .isEqualTo(emptyOutput());
    }


}
