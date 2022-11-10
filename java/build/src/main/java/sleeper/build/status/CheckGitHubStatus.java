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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.build.chunks.ProjectChunk;
import sleeper.build.chunks.ProjectChunks;
import sleeper.build.chunks.ProjectConfiguration;
import sleeper.build.github.GitHubHead;
import sleeper.build.github.GitHubWorkflowRun;
import sleeper.build.github.GitHubWorkflowRuns;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CheckGitHubStatus {
    private static final Logger LOGGER = LoggerFactory.getLogger(CheckGitHubStatus.class);

    private final GitHubHead head;
    private final ProjectChunks chunks;
    private final long retrySeconds;
    private final long maxRetries;
    private final GitHubWorkflowRuns runs;

    public CheckGitHubStatus(ProjectConfiguration configuration, GitHubWorkflowRuns runs) {
        this.head = configuration.getHead();
        this.chunks = configuration.getChunks();
        this.retrySeconds = configuration.getRetrySeconds();
        this.maxRetries = configuration.getMaxRetries();
        this.runs = runs;
    }

    public ChunksStatus checkStatus() {
        return fetchChunksStatus(this::getStatusFromChunkWorkflow);
    }

    public WorkflowStatus checkStatusSingleWorkflow(String workflow) {
        ChunksStatus chunksStatus = fetchChunksStatus(chunk -> getStatusFromWorkflow(chunk, workflow));
        return new WorkflowStatus(chunksStatus,
                chunks.stream()
                        .map(ProjectChunk::getId)
                        .collect(Collectors.toList()));
    }

    private ChunksStatus fetchChunksStatus(Function<ProjectChunk, ChunkStatus> getChunkStatus) {
        return ChunksStatus.chunksForHead(head, listChunkStatusInOrder(getChunkStatus));
    }

    private List<ChunkStatus> listChunkStatusInOrder(Function<ProjectChunk, ChunkStatus> getChunkStatus) {
        // Since checks are done in parallel, re-order them after they are complete
        Map<String, ChunkStatus> statusByChunkId = retrieveStatusByChunkId(getChunkStatus);
        return chunks.stream()
                .map(chunk -> statusByChunkId.get(chunk.getId()))
                .collect(Collectors.toList());
    }

    private Map<String, ChunkStatus> retrieveStatusByChunkId(Function<ProjectChunk, ChunkStatus> getChunkStatus) {
        return chunks.stream().parallel()
                .map(getChunkStatus)
                .map(this::waitForOldBuild)
                .collect(Collectors.toMap(ChunkStatus::getChunkId, c -> c));
    }

    private ChunkStatus waitForOldBuild(ChunkStatus status) {
        try {
            for (int retries = 0;
                 status.isWaitForOldBuildWithHead(head)
                         && retries < maxRetries;
                 retries++) {

                LOGGER.info("Waiting for old build to finish, {} retries, chunk: {}", retries, status.getChunk().getName());
                LOGGER.info("Link to old build: {}", status.getRunUrl());

                Thread.sleep(retrySeconds * 1000);

                GitHubWorkflowRun run = runs.recheckRun(head, status.getRunId());
                status = ChunkStatus.chunk(status.getChunk()).run(run).build();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return status;
    }

    private ChunkStatus getStatusFromChunkWorkflow(ProjectChunk chunk) {
        return getStatusFromWorkflow(chunk, chunk.getWorkflow());
    }

    private ChunkStatus getStatusFromWorkflow(ProjectChunk chunk, String workflow) {
        return runs.getLatestRun(head, workflow)
                .map(run -> ChunkStatus.chunk(chunk).run(run).build())
                .orElseGet(() -> ChunkStatus.chunk(chunk).noBuild());
    }
}
