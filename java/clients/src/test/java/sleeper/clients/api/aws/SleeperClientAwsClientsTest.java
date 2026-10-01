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
package sleeper.clients.api.aws;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sts.StsClient;

import sleeper.clients.util.ShutdownWrapper;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class SleeperClientAwsClientsTest {

    @Test
    void shouldCloseAllClientsIncludingSts() {
        // Given
        List<String> closed = new ArrayList<>();
        SleeperClientAwsClients clients = SleeperClientAwsClients.builder()
                .s3ClientWrapper(ShutdownWrapper.shutdown(mock(S3Client.class), () -> closed.add("s3")))
                .dynamoClientWrapper(ShutdownWrapper.shutdown(mock(DynamoDbClient.class), () -> closed.add("dynamo")))
                .sqsClientWrapper(ShutdownWrapper.shutdown(mock(SqsClient.class), () -> closed.add("sqs")))
                .stsClientWrapper(ShutdownWrapper.shutdown(mock(StsClient.class), () -> closed.add("sts")))
                .awsCredentialsProvider(mock(AwsCredentialsProvider.class))
                .build();

        // When
        clients.close();

        // Then
        assertThat(closed).containsExactlyInAnyOrder("s3", "dynamo", "sqs", "sts");
    }
}
