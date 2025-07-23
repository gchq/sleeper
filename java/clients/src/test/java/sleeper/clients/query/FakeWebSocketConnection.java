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

import sleeper.core.properties.instance.InstanceProperties;

import java.util.ArrayList;
import java.util.List;

public class FakeWebSocketConnection implements QueryWebSocketClient.Connection {
    private boolean connected = false;
    private boolean closed = false;
    private QueryWebSocketListener listener;
    private List<String> sentMessages = new ArrayList<>();
    private List<WebSocketResponse> responses;

    public QueryWebSocketClient.Adapter adapter() {
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

    public void onMessage(String message) {
        listener.onMessage(message);
    }

    public void onClose(String reason) {
        listener.onClose(reason);
        connected = false;
        closed = true;
    }

    public void onError(Exception error) {
        listener.onError(error);
    }

    public interface WebSocketResponse {
        void sendTo(FakeWebSocketConnection client);
    }
}
