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
package sleeper.dynamodb.test;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static sleeper.dynamodb.test.GenericContainerAwsV1ClientHelper.buildAwsV1Client;

public abstract class DynamoDBTestBase {
    public static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBTestBase.class);

    public static final DynamoDBContainer CONTAINER = start();
    private static final AmazonDynamoDB DYNAMO_CLIENT = buildAwsV1Client(CONTAINER, CONTAINER.getDynamoPort(), AmazonDynamoDBClientBuilder.standard());

    @SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
    protected final AmazonDynamoDB dynamoClient = DYNAMO_CLIENT;

    @SuppressFBWarnings("BC_UNCONFIRMED_CAST_OF_RETURN_VALUE")
    private static DynamoDBContainer start() {
        DynamoDBContainer container = new DynamoDBContainer()
                .withLogConsumer(outputFrame -> LOGGER.info(outputFrame.getUtf8StringWithoutLineEnding()))
                .withEnv("DEBUG", "1");
        container.start();
        return container;
    }
}
