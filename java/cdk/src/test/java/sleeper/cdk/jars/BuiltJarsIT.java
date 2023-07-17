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
package sleeper.cdk.jars;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.core.CommonTestConstants;

import java.util.UUID;

import static com.amazonaws.services.s3.model.BucketVersioningConfiguration.ENABLED;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
public class BuiltJarsIT {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3);

    protected final AmazonS3 s3 = AmazonS3ClientBuilder.standard()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                    localStackContainer.getEndpointOverride(LocalStackContainer.Service.S3).toString(),
                    localStackContainer.getRegion()))
            .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                    localStackContainer.getAccessKey(), localStackContainer.getSecretKey())))
            .build();

    private final String bucketName = UUID.randomUUID().toString();
    private final BuiltJars builtJars = new BuiltJars(s3, bucketName);

    @Test
    void shouldGetLatestVersionOfAJar() {
        s3.createBucket(new CreateBucketRequest(bucketName));
        s3.setBucketVersioningConfiguration(new SetBucketVersioningConfigurationRequest(bucketName,
                new BucketVersioningConfiguration(ENABLED)));
        String versionId = s3.putObject(bucketName, "test.jar", "data").getVersionId();

        assertThat(builtJars.getLatestVersionId(BuiltJar.fromFormat("test.jar")))
                .isEqualTo(versionId);
        assertThat(versionId).isNotNull();
    }
}
