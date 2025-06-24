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
package sleeper.clients.api.aws;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.clients.util.ShutdownWrapper;
import sleeper.clients.util.UncheckedAutoCloseable;
import sleeper.clients.util.UncheckedAutoCloseables;

import java.util.List;
import java.util.Objects;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;

/**
 * AWS clients to instantiate a Sleeper client.
 */
public class SleeperClientAwsClients implements UncheckedAutoCloseable {

    private final ShutdownWrapper<S3Client> s3ClientWrapper;
    private final ShutdownWrapper<DynamoDbClient> dynamoClientWrapper;
    private final ShutdownWrapper<SqsClient> sqsClientWrapper;

    private SleeperClientAwsClients(Builder builder) {
        s3ClientWrapper = Objects.requireNonNull(builder.s3ClientWrapper, "s3Client must not be null");
        dynamoClientWrapper = Objects.requireNonNull(builder.dynamoClientWrapper, "dynamoClient must not be null");
        sqsClientWrapper = Objects.requireNonNull(builder.sqsClientWrapper, "sqsClient must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public S3Client s3() {
        return s3ClientWrapper.get();
    }

    public DynamoDbClient dynamo() {
        return dynamoClientWrapper.get();
    }

    public SqsClient sqs() {
        return sqsClientWrapper.get();
    }

    @Override
    public void close() {
        UncheckedAutoCloseables.close(List.of(sqsClientWrapper, dynamoClientWrapper, s3ClientWrapper));
    }

    public static class Builder {
        private ShutdownWrapper<S3Client> s3ClientWrapper;
        private ShutdownWrapper<DynamoDbClient> dynamoClientWrapper;
        private ShutdownWrapper<SqsClient> sqsClientWrapper;

        /**
         * Creates default clients to interact with AWS. These clients will be shut down automatically when the Sleeper
         * client is closed.
         *
         * @return this builder
         */
        public Builder defaultClients() {
            s3ClientWrapper = ShutdownWrapper.shutdown(buildAwsV2Client(S3Client.builder()), S3Client::close);
            dynamoClientWrapper = ShutdownWrapper.shutdown(buildAwsV2Client(DynamoDbClient.builder()), DynamoDbClient::close);
            sqsClientWrapper = ShutdownWrapper.shutdown(buildAwsV2Client(SqsClient.builder()), SqsClient::close);
            return this;
        }

        /**
         * Sets the AWS client to interact with S3.
         *
         * @param  s3Client the client
         * @return          this builder
         */
        public Builder s3Client(S3Client s3Client) {
            this.s3ClientWrapper = ShutdownWrapper.noShutdown(s3Client);
            return this;
        }

        /**
         * Sets the AWS client to interact with DynamoDB.
         *
         * @param  dynamoClient the client
         * @return              this builder
         */
        public Builder dynamoClient(DynamoDbClient dynamoClient) {
            this.dynamoClientWrapper = ShutdownWrapper.noShutdown(dynamoClient);
            return this;
        }

        /**
         * Sets the AWS client to interact with SQS.
         *
         * @param  sqsClient the client
         * @return           this builder
         */
        public Builder sqsClient(SqsClient sqsClient) {
            this.sqsClientWrapper = ShutdownWrapper.noShutdown(sqsClient);
            return this;
        }

        public SleeperClientAwsClients build() {
            return new SleeperClientAwsClients(this);
        }
    }
}
