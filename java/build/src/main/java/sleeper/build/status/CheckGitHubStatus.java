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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CheckGitHubStatus {
    private static final Logger LOGGER = LoggerFactory.getLogger(CheckGitHubStatus.class);

    private final GitHubHead head;
    private final ProjectChunks chunks;
    private final long retrySeconds;
    private final long maxRetries;
    private final GitHubStatusProvider gitHub;

    public CheckGitHubStatus(ProjectConfiguration configuration, GitHubStatusProvider gitHub) {
        this.head = configuration.getHead();
        this.chunks = configuration.getChunks();
        this.retrySeconds = configuration.getRetrySeconds();
        this.maxRetries = configuration.getMaxRetries();
        this.gitHub = gitHub;
    }

    public ChunksStatus checkStatus() {
        return ChunksStatus.chunksForHead(head, listChunkStatusInOrder());
    }

    private List<ChunkStatus> listChunkStatusInOrder() {
        // Since checks are done in parallel, re-order them after they are complete
        Map<String, ChunkStatus> statusByChunkId = retrieveStatusByChunkId();
        return chunks.stream()
                .map(chunk -> statusByChunkId.get(chunk.getId()))
                .collect(Collectors.toList());
    }

    private Map<String, ChunkStatus> retrieveStatusByChunkId() {
        return chunks.stream().parallel()
                .map(this::retrieveStatusWaitingForOldBuilds)
                .collect(Collectors.toMap(ChunkStatus::getChunkId, c -> c));
    }

    private ChunkStatus retrieveStatusWaitingForOldBuilds(ProjectChunk chunk) {
        ChunkStatus status = gitHub.workflowStatus(head, chunk);
        try {
            for (int retries = 0;
                 status.isWaitForOldBuildWithHead(head)
                         && retries < maxRetries;
                 retries++) {

                LOGGER.info("Waiting for old build to finish, {} retries, chunk: {}", retries, chunk.getName());
                LOGGER.info("Link to old build: {}", status.getRunUrl());

                Thread.sleep(retrySeconds * 1000);
                status = gitHub.recheckRun(head, status);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return status;
    }
}
