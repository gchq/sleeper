/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.build.util;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

public class PathUtilsTest {

    @Test
    public void shouldGetCommonPathUnderProjectRoot() {
        // Given
        Path path1 = Paths.get("/path/to/project/.github/config/chunks.yaml");
        Path path2 = Paths.get("/path/to/project/java");

        // When
        Path base = PathUtils.commonPath(path1, path2);

        // Then
        assertThat(base).isEqualTo(Paths.get("/path/to/project"));
        assertThat(base.relativize(path1)).isEqualTo(Paths.get(".github/config/chunks.yaml"));
    }

    @Test
    public void shouldGetCommonPathUnderRelativePath() {
        // Given
        Path path1 = Paths.get("path/to/project/.github/config/chunks.yaml");
        Path path2 = Paths.get("path/to/project/java");

        // When
        Path base = PathUtils.commonPath(path1, path2);

        // Then
        assertThat(base).isEqualTo(Paths.get("path/to/project"));
        assertThat(base.relativize(path1)).isEqualTo(Paths.get(".github/config/chunks.yaml"));
    }

    @Test
    public void shouldGetRootWhenNoCommonPath() {
        // Given
        Path path1 = Paths.get("/some/place");
        Path path2 = Paths.get("/other/place");

        // When
        Path base = PathUtils.commonPath(path1, path2);

        // Then
        assertThat(base).isEqualTo(Paths.get("/"));
        assertThat(base.relativize(path1)).isEqualTo(Paths.get("some/place"));
    }

    @Test
    public void shouldGetCurrentDirectoryWhenRelativePathsHaveNoCommonRoot() {
        // Given
        Path path1 = Paths.get("a/b/c");
        Path path2 = Paths.get("d/e/f");

        // When
        Path base = PathUtils.commonPath(path1, path2);

        // Then
        assertThat(base).isEqualTo(Paths.get(""));
        assertThat(base.relativize(path1)).isEqualTo(path1);
        assertThat(Files.isDirectory(base)).isTrue();
    }
}
