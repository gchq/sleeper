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

package sleeper.utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.util.ClientUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

public class ClientUtilsFilesTest {
    @TempDir
    private Path tempDir;

    @Test
    void shouldRemoveFileIfExists() throws IOException {
        // Given
        Path newFile = tempDir.resolve("newFile");
        Files.createFile(newFile);

        // When
        ClientUtils.clearDirectory(tempDir);

        // Then
        assertThat(newFile).doesNotExist();
    }

    @Test
    void shouldNotRemoveRootDirectory() {
        // Given/When
        ClientUtils.clearDirectory(tempDir);

        // Then
        assertThat(tempDir).exists();
    }

    @Test
    void shouldRemoveMultipleDirectoriesAndFiles() throws IOException {
        // Given
        Files.createDirectory(tempDir.resolve("dir1"));
        Files.createDirectory(tempDir.resolve("dir1/nested1"));
        Files.createFile(tempDir.resolve("dir1/nested1/file1"));
        Files.createDirectory(tempDir.resolve("dir2"));
        Files.createDirectory(tempDir.resolve("dir2/nested2"));
        Files.createFile(tempDir.resolve("dir2/nested2/file2"));

        // When
        ClientUtils.clearDirectory(tempDir);

        // Then
        assertThat(tempDir).isEmptyDirectory();
    }
}
