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
package sleeper.systemtest.drivers.util;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.lambda.LambdaClient;

import sleeper.clients.deploy.InvokeLambda;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.InstanceProperty;
import sleeper.systemtest.configuration.SystemTestProperties;

import java.io.IOException;
import java.time.Duration;

public class InvokeSystemTestLambda {

    private InvokeSystemTestLambda() {
    }

    public static void forInstance(String instanceId, InstanceProperty lambdaFunctionProperty) throws IOException {
        try (LambdaClient lambdaClient = createSystemTestLambdaClient()) {
            AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
            SystemTestProperties systemTestProperties = new SystemTestProperties();
            systemTestProperties.loadFromS3GivenInstanceId(s3Client, instanceId);
            s3Client.shutdown();
            client(lambdaClient, systemTestProperties).invokeLambda(lambdaFunctionProperty);
        }
    }

    public static Client client(LambdaClient lambdaClient, InstanceProperties instanceProperties) {
        return lambdaFunctionProperty ->
                InvokeLambda.invokeWith(lambdaClient, instanceProperties.get(lambdaFunctionProperty));
    }

    public static LambdaClient createSystemTestLambdaClient() {
        return LambdaClient.builder()
                .overrideConfiguration(builder -> builder
                        .apiCallTimeout(Duration.ofMinutes(5))
                        .apiCallAttemptTimeout(Duration.ofMinutes(5)))
                .httpClientBuilder(ApacheHttpClient.builder()
                        .socketTimeout(Duration.ofMinutes(5)))
                .build();
    }

    public interface Client {
        void invokeLambda(InstanceProperty lambdaFunctionProperty);
    }
}
