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

import static org.assertj.core.api.Assertions.assertThat;

public class ChunksStatusTest {

    @Test
    public void shouldReportAndPassWhenTwoChunksSuccessful() {
        ChunksStatus status = ChunksStatus.chunksForHead(TestProperties.exampleHead(),
                ChunkStatus.success("common"),
                ChunkStatus.success("data"));

        assertThat(status.isFailCheck()).isFalse();
        assertThat(status.reportString()).isEqualTo("" +
                "common: completed, success\n" +
                "data: completed, success\n");
    }

    @Test
    public void shouldReportAndPassWhenOneChunkSuccessfulOneInProgressOnHeadSha() {
        GitHubHead head = TestProperties.exampleHead();
        ChunksStatus status = ChunksStatus.chunksForHead(head,
                ChunkStatus.success("common"),
                ChunkStatus.chunk("data").commitSha(head.getSha()).inProgress());

        assertThat(status.isFailCheck()).isFalse();
        assertThat(status.reportString()).isEqualTo("" +
                "common: completed, success\n" +
                "data: in_progress\n" +
                "Build is for current commit\n");
    }

    @Test
    public void shouldReportAndFailWhenOneChunkSuccessfulOneInProgressOnOldSha() {
        GitHubHead head = TestProperties.exampleHead();
        ChunksStatus status = ChunksStatus.chunksForHead(head,
                ChunkStatus.success("common"),
                ChunkStatus.chunk("data").commitSha("old-sha").inProgress());

        assertThat(status.isFailCheck()).isTrue();
        assertThat(status.reportString()).isEqualTo("" +
                "common: completed, success\n" +
                "data: in_progress\n" +
                "Commit: old-sha\n");
    }

    @Test
    public void shouldReportAndFailWhenOneChunkSuccessfulOneFailed() {
        ChunksStatus status = ChunksStatus.chunksForHead(TestProperties.exampleHead(),
                ChunkStatus.success("common"),
                ChunkStatus.failure("data"));

        assertThat(status.isFailCheck()).isTrue();
        assertThat(status.reportString()).isEqualTo("" +
                "common: completed, success\n" +
                "data: completed, failure\n");
    }

    @Test
    public void shouldReportAndFailWhenOneChunkSuccessfulOneCancelled() {
        ChunksStatus status = ChunksStatus.chunksForHead(TestProperties.exampleHead(),
                ChunkStatus.success("common"),
                ChunkStatus.cancelled("data"));

        assertThat(status.isFailCheck()).isTrue();
        assertThat(status.reportString()).isEqualTo("" +
                "common: completed, success\n" +
                "data: completed, cancelled\n");
    }

    @Test
    public void shouldReportAndPassWhenNoChunksHaveBuilds() {
        ChunksStatus status = ChunksStatus.chunksForHead(TestProperties.exampleHead(),
                ChunkStatus.noBuild("common"),
                ChunkStatus.noBuild("data"));

        assertThat(status.isFailCheck()).isFalse();
        assertThat(status.reportString()).isEqualTo("" +
                "common: null\n" +
                "data: null\n");
    }
}
