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
package sleeper.cdk;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.properties.InstanceProperties;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;

class BuiltJarTest {

    @TempDir
    private Path tempDir;

    @Test
    void shouldPopulateFilenameWithVersion() throws IOException {
        // Given
        BuiltJar jar = withProperties("sleeper.version=1.0")
                .jar(new BuiltJar.Jar("test-%s.jar"));

        // When / Then
        assertThat(jar.fileName())
                .isEqualTo("test-1.0.jar");
    }

    @Test
    void shouldComputeShaForAFile() throws IOException, NoSuchAlgorithmException {
        // Given
        BuiltJar jar = withProperties("sleeper.version=1.0")
                .jar(new BuiltJar.Jar("test-%s.jar"));
        Files.writeString(tempDir.resolve("test-1.0.jar"), "foobar");

        // When / Then
        assertThat(jar.codeSha256())
                .isEqualTo("w6uP8Tcg6K2QR905Rms8iXTlksL6OD1KOWBxTK7wxPI=");
    }

    private BuiltJar.Context withProperties(String propertiesString) throws IOException {
        InstanceProperties properties = createTestInstanceProperties();
        properties.loadFromString(propertiesString);
        return BuiltJar.withPropertiesAndDirectory(properties, tempDir);
    }
}
