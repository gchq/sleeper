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

import sleeper.clients.util.UncheckedAutoCloseable;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * A wrapper for an object that tracks whether it should be shut down.
 *
 * @param <T> the type of the object to shut down
 */
public class ShutdownWrapper<T> implements UncheckedAutoCloseable {

    private final T toShutdown;
    private final Runnable shutdown;

    private ShutdownWrapper(T toShutdown, Runnable shutdown) {
        this.toShutdown = Objects.requireNonNull(toShutdown, "toShutdown must not be null");
        this.shutdown = Objects.requireNonNull(shutdown, "shutdown must not be null");
    }

    /**
     * Creates a wrapper that will not shut down the object.
     *
     * @param  <T>  the type of the object
     * @param  wrap the object to wrap
     * @return      the wrapper
     */
    public static <T> ShutdownWrapper<T> noShutdown(T wrap) {
        return new ShutdownWrapper<T>(wrap, () -> {
        });
    }

    /**
     * Creates a wrapper that will shut down the object.
     *
     * @param  <T>      the type of the object
     * @param  wrap     the object to wrap
     * @param  shutdown the method to call on the object to shut it down
     * @return          the wrapper
     */
    public static <T> ShutdownWrapper<T> shutdown(T wrap, Consumer<T> shutdown) {
        return new ShutdownWrapper<T>(wrap, () -> shutdown.accept(wrap));
    }

    public T get() {
        return toShutdown;
    }

    @Override
    public void close() {
        shutdown.run();
    }

}
