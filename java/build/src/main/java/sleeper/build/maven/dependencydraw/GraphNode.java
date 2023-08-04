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

package sleeper.build.maven.dependencydraw;

import sleeper.build.maven.ArtifactReference;
import sleeper.build.maven.MavenModuleAndPath;

public class GraphNode {

    private final MavenModuleAndPath module;

    public GraphNode(MavenModuleAndPath module) {
        this.module = module;
    }

    public String toString() {
        return module.getPath();
    }

    public MavenModuleAndPath getModule() {
        return module;
    }

    public ArtifactReference getArtifactReference() {
        return module.artifactReference();
    }
}
