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

import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.OptionalStack;
import sleeper.core.table.TableStatus;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;

public class ResourceUtils {

    private ResourceUtils() {
    }

    /**
     * Loads the properties for the instance, checking that the given optional stack is enabled. If the stack is not
     * enabled a 404 Not Found is thrown, with the error code and message derived from the stack name (e.g.
     * {@link OptionalStack#QueryStack} produces the error code "query_not_enabled").
     *
     * @param  s3Client                the S3 client
     * @param  accountName             the AWS account name the instance is deployed in
     * @param  instanceId              the instance id
     * @param  stack                   the optional stack that must be enabled
     * @return                         the instance properties
     * @throws WebApplicationException a 404 Not Found if the stack is not enabled for the instance
     */
    public static InstanceProperties loadPropertiesAndCheckStackEnabled(S3Client s3Client, String accountName, String instanceId, OptionalStack stack) {
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, instanceId);
        if (!instanceProperties.getEnumList(OPTIONAL_STACKS, OptionalStack.class).contains(stack)) {
            String words = stackNameAsWords(stack);
            throw notAvailable(words.replace(' ', '_') + "_not_enabled", "The " + words + " stack is not enabled for this instance.");
        }
        return instanceProperties;
    }

    /**
     * Converts an optional stack name into space-separated lower-case words, dropping the "Stack" suffix (e.g.
     * {@link OptionalStack#QueryStack} becomes "query").
     *
     * @param  stack the optional stack
     * @return       the stack name as lower-case words
     */
    private static String stackNameAsWords(OptionalStack stack) {
        return stack.name()
                .replaceAll("Stack$", "")
                // splits camel case into words, e.g. "QueryStack" -> "Query Stack"
                .replaceAll("(?<=[a-z0-9])(?=[A-Z])", " ")
                .toLowerCase(Locale.ROOT);
    }

    /**
     * Builds a map of table name by table id, preserving the order tables are returned by the table index.
     *
     * @param  dynamoDbClient     the DynamoDB client
     * @param  instanceProperties the instance properties
     * @return                    a map of table name keyed by table unique id
     */
    public static Map<String, String> tableNamesById(DynamoDbClient dynamoDbClient, InstanceProperties instanceProperties) {
        DynamoDBTableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoDbClient);
        return tableIndex.streamAllTables().collect(Collectors.toMap(
                TableStatus::getTableUniqueId, TableStatus::getTableName, (a, b) -> a, LinkedHashMap::new));
    }

    /**
     * Builds a 404 Not Found exception carrying a {@link NotAvailable} JSON body.
     *
     * @param  error   a machine-readable error code
     * @param  message a human-readable description of why the resource is unavailable
     * @return         the exception to throw
     */
    public static WebApplicationException notAvailable(String error, String message) {
        return new WebApplicationException(
                Response.status(Response.Status.NOT_FOUND)
                        .entity(new NotAvailable(error, message))
                        .type(MediaType.APPLICATION_JSON)
                        .build());
    }

}