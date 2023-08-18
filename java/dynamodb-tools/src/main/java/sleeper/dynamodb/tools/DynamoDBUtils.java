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
package sleeper.dynamodb.tools;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TimeToLiveSpecification;
import com.amazonaws.services.dynamodbv2.model.UpdateTimeToLiveRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

public class DynamoDBUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBUtils.class);


    private DynamoDBUtils() {
    }

    public static String instanceTableName(String instanceId, String tableName) {
        return String.join("-", "sleeper", instanceId, tableName);
    }

    public static void initialiseTable(
            AmazonDynamoDB dynamoDB,
            String tableName,
            List<AttributeDefinition> attributeDefinitions,
            List<KeySchemaElement> keySchemaElements) {

        CreateTableRequest request = new CreateTableRequest()
                .withTableName(tableName)
                .withAttributeDefinitions(attributeDefinitions)
                .withKeySchema(keySchemaElements)
                .withBillingMode(BillingMode.PAY_PER_REQUEST);
        try {
            CreateTableResult result = dynamoDB.createTable(request);
            LOGGER.info("Created table {}", result.getTableDescription().getTableName());
        } catch (ResourceInUseException e) {
            if (e.getMessage().contains("Table already exists")) {
                LOGGER.warn("Table {} already exists", tableName);
            } else {
                throw e;
            }
        }
    }

    public static void configureTimeToLive(AmazonDynamoDB dynamoDB, String tableName, String expiryField) {
        dynamoDB.updateTimeToLive(new UpdateTimeToLiveRequest()
                .withTableName(tableName)
                .withTimeToLiveSpecification(
                        new TimeToLiveSpecification()
                                .withEnabled(true)
                                .withAttributeName(expiryField)
                ));
        LOGGER.info("Configured TTL on field {}", expiryField);
    }

    public static Stream<Map<String, AttributeValue>> streamPagedItems(AmazonDynamoDB dynamoDB, ScanRequest scanRequest) {
        return streamPagedResults(dynamoDB, scanRequest)
                .flatMap(result -> result.getItems().stream());
    }

    public static Stream<Map<String, AttributeValue>> streamPagedItems(AmazonDynamoDB dynamoDB, QueryRequest queryRequest) {
        return streamPagedResults(dynamoDB, queryRequest)
                .flatMap(result ->
                        result.getItems().stream());
    }

    public static Stream<ScanResult> streamPagedResults(AmazonDynamoDB dynamoDB, ScanRequest scanRequest) {
        return streamResults(scanRequest, dynamoDB::scan,
                ScanResult::getLastEvaluatedKey, scanRequest::withExclusiveStartKey);
    }

    public static Stream<QueryResult> streamPagedResults(AmazonDynamoDB dynamoDB, QueryRequest queryRequest) {
        return streamResults(queryRequest, dynamoDB::query,
                QueryResult::getLastEvaluatedKey, queryRequest::withExclusiveStartKey);
    }

    public static <Request, Result, Key> Stream<Result> streamResults(
            Request request, Function<Request, Result> query,
            Function<Result, Key> getLastKey, Function<Key, Request> withStartKey) {
        return Stream.iterate(
                query.apply(request),
                Objects::nonNull,
                result -> Optional.ofNullable(getLastKey.apply(result))
                        .map(lastKey -> query.apply(withStartKey.apply(lastKey)))
                        .orElse(null));
    }
}
