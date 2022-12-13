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
package sleeper.build.chunks;

import org.junit.Test;
import sleeper.build.maven.MavenModuleStructure;
import sleeper.build.maven.TestMavenModuleStructure;

import static org.assertj.core.api.Assertions.assertThat;

public class GetChunkConfigTest {

    private static final ProjectChunks CHUNKS = TestChunks.example();
    private static final MavenModuleStructure MAVEN = TestMavenModuleStructure.example();

    private String getChunkProperty(String chunkId, String propertyName) {
        return GetChunkConfig.get(CHUNKS.getById(chunkId), MAVEN, propertyName);
    }

    @Test
    public void shouldGetChunkName() {
        assertThat(getChunkProperty("ingest", "name")).isEqualTo("Ingest");
    }

    @Test
    public void shouldGetChunkProjectList() {
        assertThat(getChunkProperty("bulk-import", "maven_project_list"))
                .isEqualTo("bulk-import/bulk-import-common,bulk-import/bulk-import-starter,bulk-import/bulk-import-runner");
    }

    @Test
    public void shouldGetGitHubActionsOutputs() {
        assertThat(getChunkProperty("common", "github_actions_outputs"))
                .isEqualTo("chunkName=Common\n" +
                        "moduleList=core,configuration\n" +
                        "dependencyList=core,configuration");
    }

    @Test
    public void shouldGetGitHubActionsOutputsWithDependencies() {
        assertThat(getChunkProperty("ingest", "github_actions_outputs"))
                .isEqualTo("chunkName=Ingest\n" +
                        "moduleList=ingest\n" +
                        "dependencyList=ingest,configuration,core");
    }
}
