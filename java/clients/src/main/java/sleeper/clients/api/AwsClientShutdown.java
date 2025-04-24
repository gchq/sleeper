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
package sleeper.clients.api;

import java.util.function.Consumer;

public class AwsClientShutdown<C> {

    private final C client;
    private final Runnable shutdown;

    private AwsClientShutdown(C client, Runnable shutdown) {
        this.client = client;
        this.shutdown = shutdown;
    }

    public C getClient() {
        return client;
    }

    public void shutdown() {
        shutdown.run();
    }

    public static <C> AwsClientShutdown<C> noShutdown(C client) {
        return new AwsClientShutdown<C>(client, () -> {
        });
    }

    public static <C> AwsClientShutdown<C> shutdown(C client, Consumer<C> shutdown) {
        return new AwsClientShutdown<C>(client, () -> shutdown.accept(client));
    }

}
