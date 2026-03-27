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
package sleeper.foreign.datafusion;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

public class FFISleeperOptionsTest {
    public static final jnr.ffi.Runtime RUNTIME = jnr.ffi.Runtime.getSystemRuntime();

    @Test
    void shouldFailOnZeroRowGroupSize() {
        // Given
        FFISleeperOptions options = new FFISleeperOptions(RUNTIME);
        options.max_row_group_size.set(0);

        // Then
        assertThatIllegalStateException().isThrownBy(() -> options.validate()).withMessage("max row group size < 1");
    }

    @Test
    void shouldFailOnZeroPageSize() {
        // Given
        FFISleeperOptions options = new FFISleeperOptions(RUNTIME);
        options.max_page_size.set(0);

        // Then
        assertThatIllegalStateException().isThrownBy(() -> options.validate()).withMessage("max page size < 1");
    }

    @Test
    void shouldFailOnZeroColumnTruncateLength() {
        // Given
        FFISleeperOptions options = new FFISleeperOptions(RUNTIME);
        options.column_truncate_length.set(0);

        // Then
        assertThatIllegalStateException().isThrownBy(() -> options.validate()).withMessage("column truncate length < 1");
    }

    @Test
    void shouldFailOnZeroStatsTruncateLength() {
        // Given
        FFISleeperOptions options = new FFISleeperOptions(RUNTIME);
        options.stats_truncate_length.set(0);

        // Then
        assertThatIllegalStateException().isThrownBy(() -> options.validate()).withMessage("stats truncate length < 1");
    }

    @Test
    void shouldFailOnNullCompressionCodec() {
        // Given
        FFISleeperOptions options = new FFISleeperOptions(RUNTIME);
        options.compression.set(null);

        // Then
        assertThatNullPointerException().isThrownBy(() -> options.validate()).withMessage("Parquet compression codec is null");
    }

    @Test
    void shouldFailOnNullWriterVersion() {
        // Given
        FFISleeperOptions options = new FFISleeperOptions(RUNTIME);
        options.writer_version.set(null);

        // Then
        assertThatNullPointerException().isThrownBy(() -> options.validate()).withMessage("Parquet writer is null");
    }

    @Test
    void shouldValidate() {
        // Given
        FFISleeperOptions options = new FFISleeperOptions(RUNTIME);
        options.max_row_group_size.set(10);
        options.max_page_size.set(10);
        options.column_truncate_length.set(10);
        options.stats_truncate_length.set(10);
        options.compression.set("zstd");
        options.writer_version.set("v2");

        // When
        options.validate();

        // Then - pass
    }
}
