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
import sleeper.configuration.properties.local.SaveLocalProperties;

import java.io.IOException;
import java.nio.file.Path;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.cdk.UtilsTestHelper.cdkContextWithPropertiesFileAndSkipVersionCheck;
import static sleeper.cdk.UtilsTestHelper.createInstancePropertiesWithVersion;

public class UtilsVersionTest {
    @TempDir
    private Path tempDir;

    @Test
    void shouldGetVersionAsResource() {
        assertThat(Utils.getVersion())
                .matches(Pattern.compile("\\d+\\.\\d+\\.\\d+(-SNAPSHOT)?"));
    }

    @Test
    void shouldPassVersionCheckWhenLocalVersionMatchesDeployedVersion() throws IOException {
        // Given
        InstanceProperties instanceProperties = createInstancePropertiesWithVersion(Utils.getVersion());
        SaveLocalProperties.saveToDirectory(tempDir, instanceProperties, Stream.empty());

        // When
        assertThatCode(() -> Utils.loadInstanceProperties(new InstanceProperties(),
                cdkContextWithPropertiesFileAndSkipVersionCheck(tempDir, false)))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldFailVersionCheckWhenLocalVersionDoesNotMatchDeployedVersion() throws IOException {
        // Given
        InstanceProperties instanceProperties = createInstancePropertiesWithVersion("0.14.0-SNAPSHOT");
        SaveLocalProperties.saveToDirectory(tempDir, instanceProperties, Stream.empty());

        // When
        assertThatThrownBy(() -> Utils.loadInstanceProperties(new InstanceProperties(),
                cdkContextWithPropertiesFileAndSkipVersionCheck(tempDir, false)))
                .isInstanceOf(MismatchedVersionException.class);
    }

    @Test
    void shouldSkipVersionCheckWhenLocalVersionDoesNotMatchDeployedVersion() throws IOException {
        // Given
        InstanceProperties instanceProperties = createInstancePropertiesWithVersion("0.14.0-SNAPSHOT");
        SaveLocalProperties.saveToDirectory(tempDir, instanceProperties, Stream.empty());

        // When
        assertThatCode(() -> Utils.loadInstanceProperties(new InstanceProperties(),
                cdkContextWithPropertiesFileAndSkipVersionCheck(tempDir, true)))
                .doesNotThrowAnyException();
    }
}
