/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.api;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Produces;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.localstack.test.LocalStackAwsV2ClientHelper;
import sleeper.localstack.test.SleeperLocalStackContainer;

@ApplicationScoped
public class LocalStackSqsClientProducer {

    @Produces
    @ApplicationScoped
    @Alternative
    @Priority(1)
    public SqsClient sqsClient() {
        return LocalStackAwsV2ClientHelper.buildAwsV2Client(
                SleeperLocalStackContainer.INSTANCE, SqsClient.builder());
    }

}
