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
package sleeper.build.maven;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MavenModuleStructure {

    private final String artifactId;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public MavenModuleStructure(@JsonProperty("artifactId") String artifactId) {
        this.artifactId = artifactId;
    }

    public static MavenModuleStructure fromProjectBase(Path path) throws IOException {
        return fromPom(path.resolve("pom.xml"));
    }

    public static MavenModuleStructure fromPom(Path path) throws IOException {
        ObjectMapper mapper = new XmlMapper();
        try (Reader reader = Files.newBufferedReader(path)) {
            return mapper.readValue(reader, MavenModuleStructure.class);
        }
    }

    public List<String> getProjectListOfAllCompiledModules() {
        return Collections.singletonList(artifactId);
    }
}
