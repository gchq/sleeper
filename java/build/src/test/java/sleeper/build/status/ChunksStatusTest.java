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
package sleeper.build.status;

import org.junit.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class ChunksStatusTest {

    @Test
    public void shouldReportAndPassWhenTwoChunksSuccessful() throws Exception {
        Properties properties = TestProperties.example("two-chunks-successful.properties");
        ChunksStatus status = ChunksStatus.from(properties);

        assertThat(status).isEqualTo(
                ChunksStatus.chunks(
                        ChunkStatus.success("common"),
                        ChunkStatus.success("data")));
        assertThat(status.isFailCheck()).isFalse();
        assertThat(status.reportString()).isEqualTo("" +
                "common: completed, success\n" +
                "data: completed, success\n");
    }

    @Test
    public void shouldReportAndPassWhenOneChunkSuccessfulOneInProgress() throws Exception {
        Properties properties = TestProperties.example("one-chunk-successful-one-in-progress.properties");
        ChunksStatus status = ChunksStatus.from(properties);

        assertThat(status).isEqualTo(
                ChunksStatus.chunks(
                        ChunkStatus.success("common"),
                        ChunkStatus.inProgress("data")));
        assertThat(status.isFailCheck()).isFalse();
        assertThat(status.reportString()).isEqualTo("" +
                "common: completed, success\n" +
                "data: in_progress\n");
    }

    @Test
    public void shouldReportAndFailWhenOneChunkSuccessfulOneFailed() throws Exception {
        Properties properties = TestProperties.example("one-chunk-successful-one-failed.properties");
        ChunksStatus status = ChunksStatus.from(properties);

        assertThat(status).isEqualTo(
                ChunksStatus.chunks(
                        ChunkStatus.success("common"),
                        ChunkStatus.failure("data")));
        assertThat(status.isFailCheck()).isTrue();
        assertThat(status.reportString()).isEqualTo("" +
                "common: completed, success\n" +
                "data: completed, failure\n");
    }

    @Test
    public void shouldReportAndFailWhenOneChunkSuccessfulOneCancelled() throws Exception {
        Properties properties = TestProperties.example("one-chunk-successful-one-cancelled.properties");
        ChunksStatus status = ChunksStatus.from(properties);

        assertThat(status).isEqualTo(
                ChunksStatus.chunks(
                        ChunkStatus.success("common"),
                        ChunkStatus.cancelled("data")));
        assertThat(status.isFailCheck()).isTrue();
        assertThat(status.reportString()).isEqualTo("" +
                "common: completed, success\n" +
                "data: completed, cancelled\n");
    }

    @Test
    public void shouldReportAndPassWhenNoChunksHaveBuilds() throws Exception {
        Properties properties = TestProperties.example("two-chunks-missing.properties");
        ChunksStatus status = ChunksStatus.from(properties);

        assertThat(status).isEqualTo(
                ChunksStatus.chunks(
                        ChunkStatus.noBuild("common"),
                        ChunkStatus.noBuild("data")));
        assertThat(status.isFailCheck()).isFalse();
        assertThat(status.reportString()).isEqualTo("" +
                "common: null\n" +
                "data: null\n");
    }
}
