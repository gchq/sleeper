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
package sleeper.environment.cdk.buildec2;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;

import static org.assertj.core.api.Assertions.assertThat;

public class KeyPairUtilIT {
    @TempDir
    private Path tempDir;

    @Test
    void shouldWritePrivateKeyFile() throws Exception {
        // When
        Path expectedPath = tempDir.resolve("WriteKey.pem");
        KeyPairUtil.writePrivateToFile(KeyPairUtil.generate(), expectedPath.toString());

        // Then
        assertThat(Files.getPosixFilePermissions(expectedPath))
                .containsExactly(PosixFilePermission.OWNER_READ);
    }

    @Test
    void shouldOverwritePrivateKeyFile() throws Exception {
        // Given
        Path expectedPath = tempDir.resolve("OverwriteKey.pem");
        Files.createFile(expectedPath);

        // When
        KeyPairUtil.writePrivateToFile(KeyPairUtil.generate(), expectedPath.toString());

        // Then
        assertThat(Files.getPosixFilePermissions(expectedPath))
                .containsExactly(PosixFilePermission.OWNER_READ);
    }
}
