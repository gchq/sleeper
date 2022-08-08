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
package sleeper.environment.cdk.buildec2;

import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;

import static org.assertj.core.api.Assertions.assertThat;

public class KeyPairUtilTest {

    @Test
    public void can_get_public_key_base64() {
        assertThat(KeyPairUtil.publicBase64(KeyPairUtil.generate()))
                .hasSize(392);
    }

    @Test
    public void can_get_private_key_pem_string() {
        assertThat(KeyPairUtil.privatePem(KeyPairUtil.generate()))
                .startsWith("-----BEGIN RSA PRIVATE KEY-----\n")
                .endsWith("\n-----END RSA PRIVATE KEY-----\n")
                .hasLineCount(28)
                .hasSizeGreaterThan(1700);
    }

    @Test
    public void can_write_private_key_file() throws Exception {
        Path expectedPath = Paths.get("test.pem");
        try {
            KeyPairUtil.writePrivateToFile(KeyPairUtil.generate(), "test.pem");
            assertThat(Files.getPosixFilePermissions(expectedPath))
                    .containsExactly(PosixFilePermission.OWNER_READ);
        } finally {
            Files.deleteIfExists(expectedPath);
        }
    }

    @Test
    public void can_overwrite_private_key_file() throws Exception {
        Path path = Paths.get("test.pem");
        Files.createFile(path);
        try {
            KeyPairUtil.writePrivateToFile(KeyPairUtil.generate(), "test.pem");
            assertThat(Files.getPosixFilePermissions(path))
                    .containsExactly(PosixFilePermission.OWNER_READ);
        } finally {
            Files.deleteIfExists(path);
        }
    }
}
