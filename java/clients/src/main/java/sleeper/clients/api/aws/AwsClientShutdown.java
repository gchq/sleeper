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
package sleeper.clients.api.aws;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * A wrapper for an AWS client that tracks whether it should be shut down when used in a Sleeper client. Used in
 * {@link AwsSleeperClientBuilder}.
 *
 * @param <C> the type of the AWS client
 */
public class AwsClientShutdown<C> {

    private final C client;
    private final Runnable shutdown;

    private AwsClientShutdown(C client, Runnable shutdown) {
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.shutdown = Objects.requireNonNull(shutdown, "shutdown must not be null");
    }

    /**
     * Creates a wrapper that will not shut down the AWS client.
     *
     * @param  <C>    the type of the AWS client
     * @param  client the AWS client
     * @return        the wrapper
     */
    public static <C> AwsClientShutdown<C> noShutdown(C client) {
        return new AwsClientShutdown<C>(client, () -> {
        });
    }

    /**
     * Creates a wrapper that will shut down the AWS client.
     *
     * @param  <C>      the type of the AWS client
     * @param  client   the AWS client
     * @param  shutdown the method to call on the client to shut it down
     * @return          the wrapper
     */
    public static <C> AwsClientShutdown<C> shutdown(C client, Consumer<C> shutdown) {
        return new AwsClientShutdown<C>(client, () -> shutdown.accept(client));
    }

    public C getClient() {
        return client;
    }

    /**
     * Shuts down the AWS client if it should be shut down when the Sleeper client is closed.
     */
    public void shutdown() {
        shutdown.run();
    }

}
