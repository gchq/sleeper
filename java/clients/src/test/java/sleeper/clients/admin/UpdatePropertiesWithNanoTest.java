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
package sleeper.clients.admin;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.util.RunCommand;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.PropertiesUtils.loadProperties;
import static sleeper.utils.RunCommandTestHelper.commandRunOn;

class UpdatePropertiesWithNanoTest {

    @TempDir
    private Path tempDir;

    private Path expectedInstancePropertiesFile;

    @BeforeEach
    void setUp() {
        expectedInstancePropertiesFile = tempDir.resolve("sleeper/admin/instance.properties");
    }

    @Test
    void shouldInvokeNanoOnInstancePropertiesFile() throws Exception {
        // Given
        InstanceProperties properties = createTestInstanceProperties();

        // When / Then
        assertThat(updateInstancePropertiesGetCommandRun(properties))
                .containsExactly("nano", expectedInstancePropertiesFile.toString());
    }

    @Test
    void shouldWriteInstancePropertiesFile() throws Exception {
        // Given
        InstanceProperties properties = createTestInstanceProperties();

        // When / Then
        assertThat(updateInstancePropertiesGetPropertiesWritten(properties))
                .isEqualTo(properties);
    }

    private String[] updateInstancePropertiesGetCommandRun(InstanceProperties properties) throws Exception {
        return commandRunOn(runCommand ->
                updateProperties(properties, runCommand));
    }

    private InstanceProperties updateInstancePropertiesGetPropertiesWritten(InstanceProperties properties) throws Exception {
        AtomicReference<InstanceProperties> foundProperties = new AtomicReference<>();
        updateProperties(properties, command -> {
            foundProperties.set(new InstanceProperties(loadProperties(expectedInstancePropertiesFile)));
            return 0;
        });
        return foundProperties.get();
    }

    private void updateProperties(InstanceProperties properties, RunCommand runCommand) throws IOException, InterruptedException {
        new UpdatePropertiesWithNano(tempDir).updateProperties(properties, runCommand);
    }
}
