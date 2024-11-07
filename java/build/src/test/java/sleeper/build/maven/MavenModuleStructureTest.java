/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.build.maven;

import org.junit.jupiter.api.Test;

import sleeper.build.testutil.TestProjectStructure;

import static org.assertj.core.api.Assertions.assertThat;

public class MavenModuleStructureTest {

    @Test
    public void shouldLoadExampleStructureFromPomFiles() throws Exception {
        // When / Then
        assertThat(TestProjectStructure.example().loadMavenStructure())
                .isEqualTo(TestMavenModuleStructure.example());
    }

    @Test
    public void shouldProduceListOfJavaModulesForMavenProjectListArguments() {
        // When / Then
        assertThat(TestMavenModuleStructure.example().allJavaModules())
                .extracting(MavenModuleAndPath::getPath).containsExactly(
                        "core", "configuration", "ingest",
                        "bulk-import/bulk-import-core",
                        "bulk-import/bulk-import-runner",
                        "bulk-import/bulk-import-starter");
    }

    @Test
    public void shouldProduceListOfAllModules() {
        // When / Then
        assertThat(TestMavenModuleStructure.example().allModules())
                .extracting(MavenModuleAndPath::getPath).containsExactly(
                        "core", "configuration", "ingest",
                        "bulk-import/bulk-import-core",
                        "bulk-import/bulk-import-runner",
                        "bulk-import/bulk-import-starter",
                        "distribution");
    }
}
