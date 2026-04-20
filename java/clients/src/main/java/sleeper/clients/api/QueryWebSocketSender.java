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
package sleeper.clients.api;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import sleeper.clients.query.QueryWebSocketClient;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.row.Row;
import sleeper.query.core.model.Query;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Queries a Sleeper table via a web socket.
 */
@FunctionalInterface
public interface QueryWebSocketSender {

    /**
     * Makes a query against a Sleeper table.
     *
     * @param  query                the query
     * @return                      the results
     * @throws InterruptedException thrown if the thread is interrupted while waiting for results
     */
    CompletableFuture<List<Row>> sendQuery(Query query) throws InterruptedException;

    /**
     * Creates an object to query a given Sleeper instance in AWS. The instance must have the optional stack for web
     * socket queries enabled.
     *
     * @param  instanceProperties      the instance properties
     * @param  tablePropertiesProvider the table properties provider
     * @param  awsCredentialsProvider  the provider for AWS credentials
     * @return                         the object
     */
    static QueryWebSocketSender query(
            InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider,
            AwsCredentialsProvider awsCredentialsProvider) {
        QueryWebSocketClient client = new QueryWebSocketClient(instanceProperties, tablePropertiesProvider, awsCredentialsProvider);
        return query -> {
            return client.submitQuery(query);
        };
    }
}
