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
package sleeper.build.status;

import org.junit.jupiter.api.Test;

import sleeper.build.github.GitHubHead;
import sleeper.build.github.TestGitHubHead;

import static org.assertj.core.api.Assertions.assertThat;

public class ChunkStatusesTest {

    @Test
    public void shouldReportAndPassWhenTwoChunksSuccessful() throws Exception {
        ChunkStatuses status = ChunkStatuses.chunksForHead(TestGitHubHead.exampleFromProperties(),
                ChunkStatus.chunk("common").success(),
                ChunkStatus.chunk("data").success());

        assertThat(status.isFailCheck()).isFalse();
        assertThat(status.reportLines()).containsExactly("",
                "common: completed, failure",
                "",
                "data: completed, success");
    }

    @Test
    public void shouldReportAndPassWhenOneChunkSuccessfulOneInProgressOnHeadSha() throws Exception {
        GitHubHead head = TestGitHubHead.exampleFromProperties();
        ChunkStatuses status = ChunkStatuses.chunksForHead(head,
                ChunkStatus.chunk("common").success(),
                ChunkStatus.chunk("data").inProgressWithSha(head.getSha()));

        assertThat(status.isFailCheck()).isFalse();
        assertThat(status.reportLines()).containsExactly("",
                "common: completed, success",
                "",
                "data: in_progress",
                "Build is for current commit");
    }

    @Test
    public void shouldReportAndFailWhenOneChunkSuccessfulOneInProgressOnOldSha() throws Exception {
        GitHubHead head = TestGitHubHead.exampleFromProperties();
        ChunkStatuses status = ChunkStatuses.chunksForHead(head,
                ChunkStatus.chunk("common").success(),
                ChunkStatus.chunk("data").inProgressWithSha("old-sha"));

        assertThat(status.isFailCheck()).isTrue();
        assertThat(status.reportLines()).containsExactly("",
                "common: completed, success",
                "",
                "data: in_progress",
                "Commit: old-sha");
    }

    @Test
    public void shouldReportAndFailWhenOneChunkSuccessfulOneFailed() throws Exception {
        ChunkStatuses status = ChunkStatuses.chunksForHead(TestGitHubHead.exampleFromProperties(),
                ChunkStatus.chunk("common").success(),
                ChunkStatus.chunk("data").failure());

        assertThat(status.isFailCheck()).isTrue();
        assertThat(status.reportLines()).containsExactly("",
                "common: completed, success",
                "",
                "data: completed, failure");
    }

    @Test
    public void shouldReportAndFailWhenOneChunkSuccessfulOneCancelled() throws Exception {
        ChunkStatuses status = ChunkStatuses.chunksForHead(TestGitHubHead.exampleFromProperties(),
                ChunkStatus.chunk("common").success(),
                ChunkStatus.chunk("data").cancelled());

        assertThat(status.isFailCheck()).isTrue();
        assertThat(status.reportLines()).containsExactly("",
                "common: completed, success",
                "",
                "data: completed, cancelled");
    }

    @Test
    public void shouldReportAndPassWhenNoChunksHaveBuilds() throws Exception {
        ChunkStatuses status = ChunkStatuses.chunksForHead(TestGitHubHead.exampleFromProperties(),
                ChunkStatus.chunk("common").noBuild(),
                ChunkStatus.chunk("data").noBuild());

        assertThat(status.isFailCheck()).isFalse();
        assertThat(status.reportLines()).containsExactly("",
                "common: null",
                "",
                "data: null");
    }
}
