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
package sleeper.cdk.jars;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.assertions.Template;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;

import sleeper.cdk.artefacts.SleeperArtefacts;
import sleeper.cdk.artefacts.SleeperArtefactsFromProperties;
import sleeper.cdk.artefacts.SleeperJarVersionIdsCache;
import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.deploy.LambdaJar;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.localstack.test.LocalStackTestBase;

import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

public class SleeperJarVersionIdsCacheIT extends LocalStackTestBase {

    private final String bucketName = UUID.randomUUID().toString();
    private final InstanceProperties instanceProperties = createInstanceProperties();

    @BeforeEach
    void setUp() {
        createBucket(bucketName);
        s3Client.putBucketVersioning(put -> put.bucket(bucketName)
                .versioningConfiguration(config -> config.status(BucketVersioningStatus.ENABLED)));
    }

    @Test
    void shouldGetLatestVersionOfAJar() {
        // Given
        String versionId = putObject(bucketName, "test.jar", "data").versionId();
        LambdaJar jar = LambdaJar.builder()
                .filenameFormat("test.jar")
                .imageName("test-lambda")
                .artifactId("test-lambda")
                .build();

        // When
        String foundVersionId = jars().getLatestVersionId(jar);

        assertThat(foundVersionId).isEqualTo(versionId);
        assertThat(foundVersionId).isNotNull();
    }

    @Test
    void shouldIncludeVersionNumberInFilenameWhenPropertyIsSetAfterJarsConstruction() {
        // Given
        SleeperJarVersionIdsCache jars = jars();
        String versionId = putObject(bucketName, "test-0.1.2.jar", "data").versionId();
        LambdaJar jar = LambdaJar.builder()
                .filenameFormat("test-%s.jar")
                .imageName("test-lambda")
                .artifactId("test-lambda")
                .build();
        instanceProperties.set(VERSION, "0.1.2");

        // When
        String foundVersionId = jars.getLatestVersionId(jar);

        assertThat(foundVersionId).isEqualTo(versionId);
        assertThat(foundVersionId).isNotNull();
    }

    @Test
    void shouldIncludeVersionNumberInLambdaCodeWhenSetAfterJarsConstruction() {
        // Given
        String objectVersionId = putObject(bucketName, "test-0.1.2.jar", "data").versionId();
        LambdaJar jar = LambdaJar.builder()
                .filenameFormat("test-%s.jar")
                .imageName("test-lambda")
                .artifactId("test-lambda")
                .build();
        LambdaHandler handler = LambdaHandler.builder()
                .jar(jar)
                .handler("my.Handler")
                .core().build();

        Stack stack = new Stack();
        instanceProperties.set(VERSION, "0.1.2");

        // When
        lambdaCode(stack).buildFunction(stack, handler, "Function",
                builder -> builder.functionName("test-function"));

        // Then
        assertThat(Template.fromStack(stack).findResources("AWS::Lambda::Function"))
                .extractingFromEntries(Entry::getValue).singleElement().asInstanceOf(InstanceOfAssertFactories.MAP)
                .extractingByKey("Properties", InstanceOfAssertFactories.MAP)
                .extractingByKey("Code", InstanceOfAssertFactories.MAP)
                .isEqualTo(Map.of(
                        "S3Bucket", bucketName,
                        "S3Key", "test-0.1.2.jar",
                        "S3ObjectVersion", objectVersionId));
    }

    private InstanceProperties createInstanceProperties() {
        InstanceProperties properties = createTestInstanceProperties();
        properties.set(JARS_BUCKET, bucketName);
        return properties;
    }

    private SleeperJarVersionIdsCache jars() {
        return SleeperJarVersionIdsCache.from(s3Client, instanceProperties);
    }

    private SleeperLambdaCode lambdaCode(Stack stack) {
        SleeperArtefacts artefacts = new SleeperArtefactsFromProperties(instanceProperties, jars());
        return artefacts.lambdaCodeAtScope(stack);
    }
}
