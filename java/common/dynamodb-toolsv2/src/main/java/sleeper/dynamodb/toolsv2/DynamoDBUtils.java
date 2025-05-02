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
package sleeper.dynamodb.toolsv2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.Tag;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveSpecification;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.UpdateTimeToLiveRequest;

import sleeper.core.util.PollWithRetries;

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
            DynamoDbClient dynamoDB,
            String tableName,
            List<AttributeDefinition> attributeDefinitions,
            List<KeySchemaElement> keySchemaElements) {
        initialiseTable(dynamoDB, tableName, attributeDefinitions, keySchemaElements, Map.of());
    }

    public static void initialiseTable(
            DynamoDbClient dynamoDB,
            String tableName,
            List<AttributeDefinition> attributeDefinitions,
            List<KeySchemaElement> keySchemaElements,
            Map<String, String> tags) {
     LOGGER.warn("Table {} already exists", request.getTableName());
        initialiseTable(dynamoDB, tags, CreateTableRequest.builder()
                .tableName(tableName)
                .attributeDefinitions(attributeDefinitions)
                .keySchema(keySchemaElements)
                .build());
    }

    public static void initialiseTable(
            DynamoDbClient dynamoDB,
            Map<String, String> tags,
            CreateTableRequest request) {
        CreateTableRequest.Builder requestBuilder = request.toBuilder().billingMode(BillingMode.PAY_PER_REQUEST.toString());
        String message = "";
        if (!tags.isEmpty()) {
            requestBuilder.tags(tags.entrySet().stream()
                    .map(e -> Tag.builder()
                            .key(e.getKey())
                            .value(e.getValue())
                            .build())
                    .collect(Collectors.toUnmodifiableList()));
            message = " with tags " + tags;
        }
        request = requestBuilder.build();
        try {
            CreateTableResponse result = dynamoDB.createTable(request);
            LOGGER.info("Created table {} {}", result.tableDescription().tableName(), message);
        } catch (ResourceInUseException e) {
            if (e.getMessage().contains("Table already exists")) {
                LOGGER.warn("Table {} already exists", request.tableName());
            } else {
                throw e;
            }
        }
    }

    public static void configureTimeToLive(DynamoDbClient dynamoDB, String tableName, String expiryField) {
        dynamoDB.updateTimeToLive(UpdateTimeToLiveRequest.builder()
                .tableName(tableName)
                .timeToLiveSpecification(
                        TimeToLiveSpecification.builder()
                                .enabled(true)
                                .attributeName(expiryField)
                                .build())
                .build());
        LOGGER.info("Configured TTL on field {}", expiryField);
    }

    public static Stream<Map<String, AttributeValue>> streamPagedItems(DynamoDbClient dynamoDB, ScanRequest scanRequest) {
        return streamPagedResults(dynamoDB, scanRequest)
                .flatMap(result -> result.items().stream());
    }

    public static Stream<Map<String, AttributeValue>> streamPagedItems(DynamoDbClient dynamoDB, QueryRequest queryRequest) {
        return streamPagedResults(dynamoDB, queryRequest)
                .flatMap(result -> result.items().stream());
    }

    public static Stream<ScanResponse> streamPagedResults(DynamoDbClient dynamoDB, ScanRequest scanRequest) {
        return Stream.iterate(
                dynamoDB.scan(scanRequest),
                Objects::nonNull,
                response -> Optional.ofNullable(response.lastEvaluatedKey().isEmpty() ? null : response.lastEvaluatedKey())
                        .map(lastKey -> dynamoDB.scan(scanRequest.toBuilder().exclusiveStartKey(lastKey).build()))
                        .orElse(null));
    }

    public static Stream<QueryResponse> streamPagedResults(DynamoDbClient dynamoDB, QueryRequest queryRequest) {
        return Stream.iterate(
                dynamoDB.query(queryRequest),
                Objects::nonNull,
                response -> Optional.ofNullable(response.lastEvaluatedKey().isEmpty() ? null : response.lastEvaluatedKey())
                        .map(lastKey -> dynamoDB.query(queryRequest.toBuilder().exclusiveStartKey(lastKey).build()))
                        .orElse(null));
    }

    public static LoadedItemsWithLimit loadPagedItemsWithLimit(DynamoDbClient dynamoDB, int limit, final ScanRequest scanRequest) {
        ScanRequest.Builder scanBuilder = scanRequest.toBuilder();
        if (scanRequest.limit() == null || scanRequest.limit() > limit) {
            scanBuilder.limit(limit + 1);
        }
        return loadPagedItemsWithLimit(limit, streamPagedResults(dynamoDB, scanRequest),
                ScanResponse::items, ScanResponse::lastEvaluatedKey,
                startKey -> dynamoDB.scan(scanBuilder.limit(1).exclusiveStartKey(startKey).build()));
    }

    public static LoadedItemsWithLimit loadPagedItemsWithLimit(DynamoDbClient dynamoDB, int limit, QueryRequest queryRequest) {
        QueryRequest.Builder queryBuilder = queryRequest.toBuilder();
        if (queryRequest.limit() == null || queryRequest.limit() > limit) {
            queryBuilder.limit(limit + 1);
        }
        return loadPagedItemsWithLimit(limit, streamPagedResults(dynamoDB, queryRequest),
                QueryResponse::items, QueryResponse::lastEvaluatedKey,
                startKey -> dynamoDB.query(queryBuilder.limit(1).exclusiveStartKey(startKey).build()));
    }

    private static <Result> LoadedItemsWithLimit loadPagedItemsWithLimit(
            int limit, Stream<Result> results,
            Function<Result, List<Map<String, AttributeValue>>> getItems,
            Function<Result, Map<String, AttributeValue>> getLastEvaluatedKey,
            Function<Map<String, AttributeValue>, Result> getLastPageWithStartKey) {
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        for (Result result : (Iterable<Result>) results::iterator) {
            List<Map<String, AttributeValue>> pageItems = getItems.apply(result);
            int newItemsFound = items.size() + pageItems.size();
            if (newItemsFound < limit) {
                items.addAll(pageItems);
            } else {
                items.addAll(pageItems.subList(0, limit - items.size()));
                boolean moreItems;
                if (newItemsFound > limit) {
                    moreItems = true;
                } else if (getLastEvaluatedKey.apply(result) == null) {
                    moreItems = false;
                } else {
                    Result lastPage = getLastPageWithStartKey.apply(getLastEvaluatedKey.apply(result));
                    moreItems = !getItems.apply(lastPage).isEmpty();
                }
                return new LoadedItemsWithLimit(items, moreItems);
            }
        }
        return new LoadedItemsWithLimit(items, false);
    }

    public static void deleteAllDynamoTableItems(
            DynamoDbClient dynamoDB, QueryRequest queryRequest,
            UnaryOperator<Map<String, AttributeValue>> getItemKeyForDelete) {
        LOGGER.info("Deleting all items from {} Dynamo DB Table", queryRequest.tableName());
        long countOfDeletedItems = streamPagedItems(dynamoDB, queryRequest.toBuilder().limit(50).build())
                .map(item -> {
                    Map<String, AttributeValue> deleteKey = getItemKeyForDelete.apply(item);
                    return dynamoDB.deleteItem(
                            DeleteItemRequest.builder()
                                    .tableName(queryRequest.tableName())
                                    .key(deleteKey).build());
                }).count();

        LOGGER.info("{} items successfully deleted from {} Dynamo DB Table", countOfDeletedItems, queryRequest.tableName());
    }

    public static boolean hasConditionalCheckFailure(TransactionCanceledException e) {
        return e.cancellationReasons().stream().anyMatch(DynamoDBUtils::isConditionCheckFailure);
    }

    public static boolean hasConditionalCheckFailure(DynamoDbException e) {
        if (e instanceof TransactionCanceledException) {
            return hasConditionalCheckFailure((TransactionCanceledException) e);
        } else {
            return false;
        }
    }

    public static boolean isConditionCheckFailure(CancellationReason reason) {
        return "ConditionalCheckFailed".equals(reason.code());
    }

    public static boolean isThrottlingException(Throwable e) {
        do {
            if (e instanceof DynamoDbException) {
                return ((DynamoDbException) e).isThrottlingException();
            }
            e = e.getCause();
        } while (e != null);
        return false;
    }

    public static void retryOnThrottlingException(PollWithRetries pollWithRetries, Runnable runnable) throws InterruptedException {
        pollWithRetries.pollUntil("no throttling exception", () -> {
            try {
                runnable.run();
                return true;
            } catch (RuntimeException e) {
                if (DynamoDBUtils.isThrottlingException(e)) {
                    LOGGER.warn("Found DynamoDB throttling exception");
                    return false;
                }
                throw e;
            }
        });
    }
}
