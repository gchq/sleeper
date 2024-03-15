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
package sleeper.environment.cdk.buildec2;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.security.KeyPair;

import static org.assertj.core.api.Assertions.assertThat;

class KeyPairUtilIT {
    @TempDir
    private Path tempDir;

    @Test
    void shouldGenerateKeyPairAndReconstructFromPem() throws Exception {
        // Given
        KeyPair pair = KeyPairUtil.generate();
        String pem = KeyPairUtil.privatePem(pair);

        // When
        KeyPair found = KeyPairUtil.readPrivatePem(pem);

        // Then
        assertThat(found.getPublic().getEncoded()).isEqualTo(pair.getPublic().getEncoded());
        assertThat(found.getPrivate().getEncoded()).isEqualTo(pair.getPrivate().getEncoded());
    }

    @Test
    void shouldGetPublicKeyInBase64() throws Exception {
        // Given
        KeyPair pair = KeyPairUtil.generate();
        Files.writeString(tempDir.resolve("private.pem"), KeyPairUtil.privatePem(pair));
        Files.writeString(tempDir.resolve("public.base64"), KeyPairUtil.publicBase64(pair));

        // When / Then
        assertThat(KeyPairUtil.publicBase64(loadKeyPair(tempDir.resolve("private.pem"))))
                .isEqualTo(Files.readString(tempDir.resolve("public.base64")));
    }

    @Test
    void shouldBuildPemStringFromKeyPair() throws Exception {
        // Given
        KeyPair pair = KeyPairUtil.generate();
        Files.writeString(tempDir.resolve("private.pem"), KeyPairUtil.privatePem(pair));

        // When / Then
        assertThat(KeyPairUtil.privatePem(loadKeyPair(tempDir.resolve("private.pem"))))
                .isEqualTo(Files.readString(tempDir.resolve("private.pem")));
    }

    @Test
    void shouldWritePrivateKeyFile() throws Exception {
        // Given
        KeyPair pair = KeyPairUtil.generate();
        Files.writeString(tempDir.resolve("private.pem"), KeyPairUtil.privatePem(pair));

        // When
        Path expectedPath = tempDir.resolve("WriteKey.pem");
        KeyPairUtil.writePrivateToFile(loadKeyPair(tempDir.resolve("private.pem")),
                expectedPath.toString());

        // Then
        assertThat(Files.getPosixFilePermissions(expectedPath))
                .containsExactly(PosixFilePermission.OWNER_READ);
    }

    @Test
    void shouldOverwritePrivateKeyFile() throws Exception {
        // Given
        KeyPair pair = KeyPairUtil.generate();
        Files.writeString(tempDir.resolve("private.pem"), KeyPairUtil.privatePem(pair));
        Path expectedPath = tempDir.resolve("OverwriteKey.pem");
        Files.createFile(expectedPath);

        // When
        KeyPairUtil.writePrivateToFile(loadKeyPair(tempDir.resolve("private.pem")),
                expectedPath.toString());

        // Then
        assertThat(Files.getPosixFilePermissions(expectedPath))
                .containsExactly(PosixFilePermission.OWNER_READ);
    }

    private KeyPair loadKeyPair(Path path) throws IOException {
        return KeyPairUtil.readPrivatePem(Files.readString(path));
    }
}
