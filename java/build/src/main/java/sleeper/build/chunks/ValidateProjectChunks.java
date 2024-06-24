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
package sleeper.build.chunks;

import java.io.IOException;
import java.nio.file.Paths;

public class ValidateProjectChunks {

    private ValidateProjectChunks() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.out.println("Usage: <chunks.yaml path> <Maven project base path>");
            System.exit(1);
            return;
        }
        ProjectStructure project = ProjectStructure.builder()
                .chunksYamlPath(Paths.get(args[0]))
                .mavenProjectPath(Paths.get(args[1]))
                .build();
        ProjectChunks chunks = project.loadChunks();
        try {
            chunks.validate(project, System.err);
        } catch (ProjectChunksValidationException e) {
            System.exit(1);
        }
    }
}
