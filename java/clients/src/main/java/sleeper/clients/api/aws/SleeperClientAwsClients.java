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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import java.util.Objects;

import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

public class SleeperClientAwsClients {

    private final AwsClientShutdown<AmazonS3> s3ClientWrapper;
    private final AwsClientShutdown<AmazonDynamoDB> dynamoClientWrapper;
    private final AwsClientShutdown<AmazonSQS> sqsClientWrapper;

    private SleeperClientAwsClients(Builder builder) {
        s3ClientWrapper = Objects.requireNonNull(builder.s3ClientWrapper, "s3Client must not be null");
        dynamoClientWrapper = Objects.requireNonNull(builder.dynamoClientWrapper, "dynamoClient must not be null");
        sqsClientWrapper = Objects.requireNonNull(builder.sqsClientWrapper, "sqsClient must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public AwsClientShutdown<AmazonS3> s3ClientWrapper() {
        return s3ClientWrapper;
    }

    public AwsClientShutdown<AmazonDynamoDB> dynamoClientWrapper() {
        return dynamoClientWrapper;
    }

    public AwsClientShutdown<AmazonSQS> sqsClientWrapper() {
        return sqsClientWrapper;
    }

    public AmazonS3 s3Client() {
        return s3ClientWrapper.getClient();
    }

    public AmazonDynamoDB dynamoClient() {
        return dynamoClientWrapper.getClient();
    }

    public AmazonSQS sqsClient() {
        return sqsClientWrapper.getClient();
    }

    public static class Builder {
        private AwsClientShutdown<AmazonS3> s3ClientWrapper;
        private AwsClientShutdown<AmazonDynamoDB> dynamoClientWrapper;
        private AwsClientShutdown<AmazonSQS> sqsClientWrapper;

        /**
         * Creates default clients to interact with AWS. These clients will be shut down automatically when the Sleeper
         * client is closed.
         *
         * @return this builder
         */
        public Builder defaultClients() {
            s3ClientWrapper = AwsClientShutdown.shutdown(buildAwsV1Client(AmazonS3ClientBuilder.standard()), AmazonS3::shutdown);
            dynamoClientWrapper = AwsClientShutdown.shutdown(buildAwsV1Client(AmazonDynamoDBClientBuilder.standard()), AmazonDynamoDB::shutdown);
            sqsClientWrapper = AwsClientShutdown.shutdown(buildAwsV1Client(AmazonSQSClientBuilder.standard()), AmazonSQS::shutdown);
            return this;
        }

        /**
         * Sets the AWS client to interact with S3.
         *
         * @param  s3Client the client
         * @return          this builder
         */
        public Builder s3Client(AmazonS3 s3Client) {
            this.s3ClientWrapper = AwsClientShutdown.noShutdown(s3Client);
            return this;
        }

        /**
         * Sets the AWS client to interact with DynamoDB.
         *
         * @param  dynamoClient the client
         * @return              this builder
         */
        public Builder dynamoClient(AmazonDynamoDB dynamoClient) {
            this.dynamoClientWrapper = AwsClientShutdown.noShutdown(dynamoClient);
            return this;
        }

        /**
         * Sets the AWS client to interact with SQS.
         *
         * @param  sqsClient the client
         * @return           this builder
         */
        public Builder sqsClient(AmazonSQS sqsClient) {
            this.sqsClientWrapper = AwsClientShutdown.noShutdown(sqsClient);
            return this;
        }

        public SleeperClientAwsClients build() {
            return new SleeperClientAwsClients(this);
        }
    }
}
