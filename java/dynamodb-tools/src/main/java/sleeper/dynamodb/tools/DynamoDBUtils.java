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
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.Tag;
import com.amazonaws.services.dynamodbv2.model.TimeToLiveSpecification;
import com.amazonaws.services.dynamodbv2.model.UpdateTimeToLiveRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
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
        initialiseTable(dynamoDB, tableName, attributeDefinitions, keySchemaElements, Map.of());
    }

    public static void initialiseTable(
            AmazonDynamoDB dynamoDB,
            String tableName,
            List<AttributeDefinition> attributeDefinitions,
            List<KeySchemaElement> keySchemaElements,
            Map<String, String> tags) {
        initialiseTable(dynamoDB, tags, new CreateTableRequest()
                .withTableName(tableName)
                .withAttributeDefinitions(attributeDefinitions)
                .withKeySchema(keySchemaElements));
    }

    public static void initialiseTable(
            AmazonDynamoDB dynamoDB,
            Map<String, String> tags,
            CreateTableRequest request) {
        request.setBillingMode(BillingMode.PAY_PER_REQUEST.toString());
        String message = "";
        if (!tags.isEmpty()) {
            request.setTags(tags.entrySet().stream()
                    .map(e -> new Tag().withKey(e.getKey()).withValue(e.getValue()))
                    .collect(Collectors.toUnmodifiableList()));
            message = " with tags " + tags;
        }
        try {
            CreateTableResult result = dynamoDB.createTable(request);
            LOGGER.info("Created table {} {}", result.getTableDescription().getTableName(), message);
        } catch (ResourceInUseException e) {
            if (e.getMessage().contains("Table already exists")) {
                LOGGER.warn("Table {} already exists", request.getTableName());
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

    public static LoadedItemsWithLimit loadPagedItemsWithLimit(AmazonDynamoDB dynamoDB, int limit, ScanRequest scanRequest) {
        if (scanRequest.getLimit() == null || scanRequest.getLimit() > limit) {
            scanRequest.setLimit(limit + 1);
        }
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        for (ScanResult result : (Iterable<ScanResult>) () -> streamPagedResults(dynamoDB, scanRequest).iterator()) {
            List<Map<String, AttributeValue>> pageItems = result.getItems();
            int newItemsFound = items.size() + pageItems.size();
            if (newItemsFound < limit) {
                items.addAll(pageItems);
            } else {
                items.addAll(pageItems.subList(0, limit - items.size()));
                boolean moreItems;
                if (newItemsFound > limit) {
                    moreItems = true;
                } else if (result.getLastEvaluatedKey() == null) {
                    moreItems = false;
                } else {
                    ScanResult lastPage = dynamoDB.scan(scanRequest.withLimit(1)
                            .withExclusiveStartKey(result.getLastEvaluatedKey()));
                    moreItems = !lastPage.getItems().isEmpty();
                }
                return new LoadedItemsWithLimit(items, moreItems);
            }
        }
        return new LoadedItemsWithLimit(items, false);
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

    public static void deleteAllDynamoTableItems(AmazonDynamoDB dynamoDB, QueryRequest queryRequest,
                                                 UnaryOperator<Map<String, AttributeValue>> getItemKeyForDelete) {
        LOGGER.info("Deleting all items from {} Dynamo DB Table", queryRequest.getTableName());
        long countOfDeletedItems = streamPagedItems(dynamoDB, queryRequest.withLimit(50))
                .map(item -> {
                    Map<String, AttributeValue> deleteKey = getItemKeyForDelete.apply(item);
                    return dynamoDB.deleteItem(
                            new DeleteItemRequest(queryRequest.getTableName(), deleteKey));
                }).count();

        LOGGER.info("{} items successfully deleted from {} Dynamo DB Table", countOfDeletedItems, queryRequest.getTableName());
    }
}
