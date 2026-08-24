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
package sleeper.clients.query;

import sleeper.core.properties.instance.InstanceProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * A fake WebSocket connection for use in tests, simulating the behaviour of a real WebSocket connection.
 */
public class FakeWebSocketConnection implements QueryWebSocketClient.Connection {
    private boolean connected = false;
    private boolean closed = false;
    private QueryWebSocketListener listener;
    private List<String> sentMessages = new ArrayList<>();
    private List<WebSocketResponse> responses;

    /**
     * Returns an adapter backed by this fake connection.
     *
     * @return an adapter backed by this fake connection
     */
    public QueryWebSocketClient.Adapter createAdapter() {
        return this::connect;
    }

    private FakeWebSocketConnection connect(InstanceProperties instanceProperties, QueryWebSocketListener listener) throws InterruptedException {
        connected = true;
        this.listener = listener;
        listener.onOpen(this);
        responses.forEach(response -> response.sendTo(this));
        return this;
    }

    @Override
    public void close() {
        if (!closed) {
            onClose("Connection closed normally");
        }
    }

    @Override
    public void closeBlocking() throws InterruptedException {
        close();
    }

    public void setFakeResponses(WebSocketResponse... responses) {
        this.responses = List.of(responses);
    }

    @Override
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

    /**
     * Delivers a message to the listener as if it arrived from the server.
     *
     * @param message the raw message to deliver
     */
    public void onMessage(String message) {
        listener.onMessage(message);
    }

    /**
     * Simulates the server closing the connection.
     *
     * @param reason the close reason
     */
    public void onClose(String reason) {
        listener.onClose(reason);
        connected = false;
        closed = true;
    }

    /**
     * Simulates an error on the connection.
     *
     * @param error the exception to deliver
     */
    public void onError(Exception error) {
        listener.onError(error);
    }

    /**
     * A pre-defined response to be delivered to the listener when the connection is opened in tests.
     */
    public interface WebSocketResponse {
        /**
         * Sends this response to the given fake connection.
         *
         * @param client the fake connection to send to
         */
        void sendTo(FakeWebSocketConnection client);
    }
}
