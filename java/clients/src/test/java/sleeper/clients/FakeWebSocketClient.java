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
package sleeper.clients;

import org.java_websocket.framing.CloseFrame;

import sleeper.clients.QueryWebSocketClient.Client;
import sleeper.clients.QueryWebSocketClient.WebSocketMessageHandler;
import sleeper.clients.util.console.ConsoleOutput;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.query.model.Query;
import sleeper.query.model.QuerySerDe;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class FakeWebSocketClient implements Client {
    private boolean connected = false;
    private boolean closed = false;
    private WebSocketMessageHandler messageHandler;
    private List<String> sentMessages = new ArrayList<>();
    private List<WebSocketResponse> responses;

    public FakeWebSocketClient(TablePropertiesProvider tablePropertiesProvider, ConsoleOutput out) {
        this.messageHandler = new WebSocketMessageHandler(new QuerySerDe(tablePropertiesProvider), out);
    }

    public boolean connectBlocking() throws InterruptedException {
        connected = true;
        return connected;
    }

    @Override
    public void closeBlocking() throws InterruptedException {
        connected = false;
        if (!closed) {
            messageHandler.onClose(CloseFrame.NORMAL, "Connection closed normally");
            closed = true;
        }
    }

    public FakeWebSocketClient withResponses(WebSocketResponse... responses) {
        this.responses = List.of(responses);
        return this;
    }

    @Override
    public CompletableFuture<List<String>> startQueryFuture(Query query) throws InterruptedException {
        CompletableFuture<List<String>> future = new CompletableFuture<>();
        messageHandler.setFuture(future);
        connectBlocking();
        messageHandler.onOpen(query, sentMessages::add);
        responses.forEach(response -> response.sendTo(messageHandler));
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

    public interface WebSocketResponse {
        void sendTo(WebSocketMessageHandler client);
    }
}
