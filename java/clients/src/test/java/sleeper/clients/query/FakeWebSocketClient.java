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
package sleeper.clients.query;

import sleeper.clients.query.QueryWebSocketClient.Client;
import sleeper.clients.query.QueryWebSocketClient.WebSocketMessageHandler;
import sleeper.core.properties.table.TableProperties;
import sleeper.query.core.model.Query;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class FakeWebSocketClient implements Client {
    private boolean connected = false;
    private boolean closed = false;
    private WebSocketMessageHandler messageHandler;
    private List<String> sentMessages = new ArrayList<>();
    private List<WebSocketResponse> responses;

    public QueryWebSocketClient.ClientProvider provider() {
        return (instanceProperties, tableProperties) -> withTable(tableProperties);
    }

    public boolean connectBlocking() throws InterruptedException {
        connected = true;
        return connected;
    }

    @Override
    public void close() {
        if (!closed) {
            onClose("Connection closed normally");
        }
    }

    public void setFakeResponses(WebSocketResponse... responses) {
        this.responses = List.of(responses);
    }

    private FakeWebSocketClient withTable(TableProperties tableProperties) {
        messageHandler = new WebSocketMessageHandler(tableProperties.getSchema());
        return this;
    }

    @Override
    public CompletableFuture<List<String>> startQueryFuture(Query query) throws InterruptedException {
        CompletableFuture<List<String>> future = new CompletableFuture<>();
        messageHandler.setFuture(future);
        messageHandler.setCloser(this::close);
        connectBlocking();
        messageHandler.onOpen(query, sentMessages::add);
        responses.forEach(response -> response.sendTo(this));
        return future;
    }

    @Override
    public boolean hasQueryFinished() {
        return messageHandler.hasQueryFinished();
    }

    @Override
    public long getTotalRecordsReturned() {
        return messageHandler.getTotalRecordsReturned();
    }

    @Override
    public List<String> getResults(String queryId) {
        return messageHandler.getResults(queryId);
    }

    public void send(String message) {
        sentMessages.add(message);
    }

    public boolean isConnected() {
        return connected;
    }

    public boolean isClosed() {
        return closed;
    }

    public List<String> getSentMessages() {
        return sentMessages;
    }

    public void onMessage(String message) {
        messageHandler.onMessage(message);
    }

    public void onClose(String reason) {
        messageHandler.onClose(reason);
        connected = false;
        closed = true;
    }

    public void onError(Exception error) {
        messageHandler.onError(error);
    }

    public interface WebSocketResponse {
        void sendTo(FakeWebSocketClient client);
    }
}
