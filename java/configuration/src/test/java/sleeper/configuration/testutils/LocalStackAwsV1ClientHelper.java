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

package sleeper.configuration.testutils;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import org.testcontainers.containers.localstack.LocalStackContainer;

/**
 * Applies configuration for AWS SDK v1 clients to communicate with LocalStack.
 */
public class LocalStackAwsV1ClientHelper {

    private LocalStackAwsV1ClientHelper() {
    }

    /**
     * Builds an AWS SDK v1 client to interact with LocalStack.
     *
     * @param  <B>                 the builder type
     * @param  <T>                 the client type
     * @param  localStackContainer the LocalStack test container
     * @param  service             the LocalStack service the client is for
     * @param  builder             the builder
     * @return                     the client
     */
    public static <B extends AwsClientBuilder<B, T>, T> T buildAwsV1Client(LocalStackContainer localStackContainer, LocalStackContainer.Service service, B builder) {
        return builder
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        localStackContainer.getEndpointOverride(service).toString(),
                        localStackContainer.getRegion()))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                        localStackContainer.getAccessKey(), localStackContainer.getSecretKey())))
                .build();
    }

}
