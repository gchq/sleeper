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

package sleeper.clients.deploy;

import java.util.List;
import java.util.Map;

public class DockerImageConfiguration {
    private static final Map<String, String> DEFAULT_DIRECTORY_BY_STACK = Map.of(
            "IngestStack", "ingest",
            "EksBulkImportStack", "bulk-import-runner",
            "CompactionStack", "compaction-job-execution",
            "SystemTestStack", "system-test",
            "EmrServerlessBulkImportStack", "bulk-import-runner-emr-serverless"
    );

    private static final List<String> DEFAULT_BUILDX_STACKS = List.of("CompactionStack");
    private final Map<String, String> directoryByStack;
    private final List<String> buildxStacks;

    public DockerImageConfiguration() {
        this(DEFAULT_DIRECTORY_BY_STACK, DEFAULT_BUILDX_STACKS);
    }

    public boolean isDirectoryBuildX(String directory) {
        for (String stack : buildxStacks) {
            if (directoryByStack.get(stack).equals(directory)) {
                return true;
            }
        }
        return false;
    }

    public DockerImageConfiguration(Map<String, String> directoryByStack, List<String> buildxStacks) {
        this.directoryByStack = directoryByStack;
        this.buildxStacks = buildxStacks;
    }

    public static DockerImageConfiguration from(Map<String, String> directoryByStack, List<String> buildxStacks) {
        return new DockerImageConfiguration(directoryByStack, buildxStacks);
    }

    public boolean hasStack(String stack) {
        return directoryByStack.containsKey(stack);
    }

    public String getStack(String stack) {
        return directoryByStack.get(stack);
    }

    public boolean isBuildXStack(String stack) {
        return buildxStacks.contains(stack);
    }
}
