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
package sleeper.build.github.actions;

import sleeper.build.chunks.ProjectChunk;
import sleeper.build.chunks.ProjectStructure;
import sleeper.build.util.ReportableException;

import java.io.PrintStream;
import java.util.List;

public class NotAllDependenciesDeclaredException extends ReportableException {

    private final ProjectChunk chunk;
    private final List<String> unconfiguredModuleRefs;

    public NotAllDependenciesDeclaredException(ProjectChunk chunk, List<String> unconfiguredModuleRefs) {
        super("Maven dependencies not declared in on.push.paths for chunk \"" + chunk.getId() + "\": " + unconfiguredModuleRefs);
        this.chunk = chunk;
        this.unconfiguredModuleRefs = unconfiguredModuleRefs;
    }

    public String getChunkId() {
        return chunk.getId();
    }

    public List<String> getUnconfiguredModuleRefs() {
        return unconfiguredModuleRefs;
    }

    @Override
    public void report(PrintStream out, ProjectStructure project) {
        out.println(getMessage());
        out.println("Please add the necessary on.push.paths at " + project.workflowPathInRepository(chunk));
    }
}
