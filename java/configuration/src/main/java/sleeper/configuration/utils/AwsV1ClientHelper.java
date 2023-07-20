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

package sleeper.configuration.utils;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class AwsV1ClientHelper {
    private static final String AWS_ENDPOINT_ENV_VAR = "AWS_ENDPOINT_URL";

    private AwsV1ClientHelper() {
    }

    public static <B extends AwsClientBuilder<B, T>, T> T buildAwsV1Client(B builder) {
        String endpoint = System.getenv(AWS_ENDPOINT_ENV_VAR);
        if (endpoint != null) {
            if (builder instanceof AmazonS3ClientBuilder) {
                ((AmazonS3ClientBuilder) builder).withPathStyleAccessEnabled(true);
            }
            return builder
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                            System.getenv(AWS_ENDPOINT_ENV_VAR), "us-east-1"))
                    .build();
        } else {
            return builder.build();
        }
    }
}
