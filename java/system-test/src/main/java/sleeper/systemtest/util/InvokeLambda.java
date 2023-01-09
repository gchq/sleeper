/*
 * Copyright 2023 Crown Copyright
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
package sleeper.systemtest.util;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import sleeper.configuration.properties.InstanceProperty;
import sleeper.systemtest.SystemTestProperties;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaClient;

import java.io.IOException;

public class InvokeLambda {

    private InvokeLambda() {
    }

    public static void forInstance(String instanceId, InstanceProperty lambdaFunctionProperty) throws IOException {

        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        SystemTestProperties systemTestProperties = new SystemTestProperties();
        systemTestProperties.loadFromS3GivenInstanceId(s3Client, instanceId);
        s3Client.shutdown();

        try (LambdaClient lambda = LambdaClient.create()) {
            lambda.invoke(builder -> builder
                    .functionName(systemTestProperties.get(lambdaFunctionProperty))
                    .payload(SdkBytes.fromUtf8String("{}")));
        }
    }
}
