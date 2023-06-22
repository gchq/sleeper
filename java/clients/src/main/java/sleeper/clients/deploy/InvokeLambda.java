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

package sleeper.clients.deploy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaClient;

public class InvokeLambda {
    private static final Logger LOGGER = LoggerFactory.getLogger(InvokeLambda.class);

    private InvokeLambda() {
    }

    public static void invokeWith(LambdaClient lambdaClient, String lambdaFunction) {
        LOGGER.info("Invoking lambda {}", lambdaFunction);
        lambdaClient.invoke(builder -> builder
                .functionName(lambdaFunction)
                .payload(SdkBytes.fromUtf8String("{}")));
    }
}
