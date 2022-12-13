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

import sleeper.build.maven.MavenModuleStructure;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Locale;
import java.util.function.Supplier;

public class GetChunkConfig {

    private final ProjectChunk chunk;
    private final Supplier<MavenModuleStructure> getMaven;
    private final Property property;

    private GetChunkConfig(ProjectChunk chunk, Supplier<MavenModuleStructure> getMaven, String property) {
        this.chunk = chunk;
        this.getMaven = getMaven;
        this.property = Property.valueOf(property.toUpperCase(Locale.ROOT));
    }

    public String getOutput() {
        switch (property) {
            case NAME:
                return chunk.getName();
            case MAVEN_PROJECT_LIST:
                return chunk.getMavenProjectList();
            case GITHUB_ACTIONS_OUTPUTS:
                return gitHubActionsOutputs(chunk, getMaven.get());
            default:
                throw new IllegalArgumentException("Unrecognised property: " + property);
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 4) {
            System.out.println("Usage: <chunk ID> <property name> <chunks.yaml path> <maven path>");
            System.out.println("Available properties (case insensitive): " + Arrays.toString(Property.values()));
            System.exit(1);
        }

        GetChunkConfig getConfig = new GetChunkConfig(
                ProjectChunksYaml.readPath(Paths.get(args[2])).getById(args[0]),
                () -> readMaven(args[3]),
                args[1]);
        System.out.println(getConfig.getOutput());
    }

    public static String get(ProjectChunk chunk, MavenModuleStructure maven, String property) {
        return new GetChunkConfig(chunk, () -> maven, property).getOutput();
    }

    private static MavenModuleStructure readMaven(String path) {
        try {
            return MavenModuleStructure.fromProjectBase(Paths.get(path));
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private enum Property {
        NAME, MAVEN_PROJECT_LIST, GITHUB_ACTIONS_OUTPUTS
    }

    private static String gitHubActionsOutputs(ProjectChunk chunk, MavenModuleStructure maven) {
        return String.format("chunkName=%s%nmoduleList=%s%ndependencyList=%s",
                chunk.getName(), chunk.getMavenProjectList(), chunk.getMavenDependencyList(maven.internalDependencies()));
    }
}
