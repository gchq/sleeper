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
package sleeper.cdk.custom;

import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import sleeper.cdk.custom.testutil.NexusRepositoryContainer;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.localstack.test.LocalStackTestBase;

import java.nio.file.Path;

import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

@Testcontainers
@WireMockTest
public class CopyJarLambdaIT extends LocalStackTestBase {

    @Container
    public final NexusRepositoryContainer source = new NexusRepositoryContainer();
    @TempDir
    private Path tempDir;
    private final InstanceProperties instanceProperties = createTestInstanceProperties();

    @BeforeEach
    void setUp() {
        createBucket(instanceProperties.get(JARS_BUCKET));
    }

    @Test
    void shouldCopyJarFromMavenToBucket() {
        source.createRepository("test");
        source.listComponents("test");
    }
}
