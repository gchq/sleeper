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
package sleeper.build.github.actions;

import sleeper.build.chunks.ProjectChunk;
import sleeper.build.chunks.ProjectStructure;
import sleeper.build.maven.InternalModuleIndex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ExpectedWorkflowTriggerPaths {

    private ExpectedWorkflowTriggerPaths() {
    }

    public static List<String> from(
            ProjectStructure project, InternalModuleIndex maven,
            ProjectChunk chunk, GitHubActionsChunkWorkflow actualWorkflow) {

        List<String> paths = new ArrayList<>(Arrays.asList(
                project.workflowPathInRepository(chunk).toString(),
                actualWorkflow.getUsesWorkflowPath().normalize().toString(),
                project.getChunksYamlRelative().toString()
        ));
        maven.ancestorsForModules(chunk.getModules())
                .map(module -> module.pomPathInRepository(project).toString())
                .forEach(paths::add);
        maven.dependenciesForModules(chunk.getModules())
                .filter(module -> !module.getStructure().isPomPackage())
                .map(module -> module.pathInRepository(project).toString() + "/**")
                .forEach(paths::add);
        return paths;
    }


}
