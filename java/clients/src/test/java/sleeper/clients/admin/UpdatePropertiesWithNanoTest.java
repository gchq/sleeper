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
import static sleeper.clients.admin.PropertiesDiffTestHelper.valueChanged;
import static sleeper.clients.deploy.GeneratePropertiesTestHelper.generateTestInstanceProperties;
import static sleeper.configuration.properties.PropertiesUtils.loadProperties;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_SOURCE_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAXIMUM_CONNECTIONS_TO_S3;
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
        InstanceProperties properties = generateTestInstanceProperties();

        // When / Then
        assertThat(updateInstancePropertiesGetCommandRun(properties))
                .containsExactly("nano", expectedInstancePropertiesFile.toString());
    }

    @Test
    void shouldWriteInstancePropertiesFile() throws Exception {
        // Given
        InstanceProperties properties = generateTestInstanceProperties();

        // When / Then
        assertThat(updateInstancePropertiesGetPropertiesWritten(properties))
                .isEqualTo(properties);
    }

    @Test
    void shouldGetDiffAfterPropertiesChanged() throws Exception {
        // Given
        InstanceProperties before = generateTestInstanceProperties();
        before.set(INGEST_SOURCE_BUCKET, "bucket-before");
        InstanceProperties after = generateTestInstanceProperties();
        after.set(INGEST_SOURCE_BUCKET, "bucket-after");

        // When / Then
        assertThat(updateInstancePropertiesGetDiff(before, after))
                .extracting(PropertiesDiff::getChanges).asList()
                .containsExactly(valueChanged(INGEST_SOURCE_BUCKET, "bucket-before", "bucket-after"));
    }

    @Test
    void shouldDetectWhenPropertiesAreInvalidAfterChange() throws Exception {
        // Given
        InstanceProperties before = generateTestInstanceProperties();
        InstanceProperties after = generateTestInstanceProperties();
        after.set(MAXIMUM_CONNECTIONS_TO_S3, "abc");

        // When / Then
        assertThat(updateInstanceProperties(before, after).isValid()).isFalse();
    }

    @Test
    void shouldDetectWhenPropertiesAreValidAfterChange() throws Exception {
        // Given
        InstanceProperties before = generateTestInstanceProperties();
        InstanceProperties after = generateTestInstanceProperties();
        after.set(MAXIMUM_CONNECTIONS_TO_S3, "12");

        // When / Then
        assertThat(updateInstanceProperties(before, after).isValid()).isTrue();
    }

    private String[] updateInstancePropertiesGetCommandRun(InstanceProperties properties) throws Exception {
        return commandRunOn(runCommand ->
                updateInstanceProperties(properties, runCommand));
    }

    private InstanceProperties updateInstancePropertiesGetPropertiesWritten(InstanceProperties properties) throws Exception {
        AtomicReference<InstanceProperties> foundProperties = new AtomicReference<>();
        updateInstanceProperties(properties, command -> {
            foundProperties.set(new InstanceProperties(loadProperties(expectedInstancePropertiesFile)));
            return 0;
        });
        return foundProperties.get();
    }

    private PropertiesDiff updateInstancePropertiesGetDiff(InstanceProperties before, InstanceProperties after) throws Exception {
        return updateInstanceProperties(before, after).getDiff();
    }

    private UpdatePropertiesRequest updateInstanceProperties(InstanceProperties before, InstanceProperties after) throws IOException, InterruptedException {
        return updateInstanceProperties(before, command -> {
            after.save(expectedInstancePropertiesFile);
            return 0;
        });
    }

    private UpdatePropertiesRequest updateInstanceProperties(InstanceProperties properties, RunCommand runCommand) throws IOException, InterruptedException {
        return new UpdatePropertiesWithNano(tempDir).updateProperties(properties, runCommand);
    }
}
