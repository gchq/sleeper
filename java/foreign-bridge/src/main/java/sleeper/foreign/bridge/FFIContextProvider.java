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
package sleeper.foreign.bridge;

import java.io.IOException;

/**
 * Provider interface for FFI contexts.
 *
 * Permits different creation strategies to be injected into classes
 * making FFI calls.
 */
@FunctionalInterface
public interface FFIContextProvider<T extends ForeignFunctions> {
    /**
     * Provides an FFI context object that is open.
     *
     * Implementations may simply choose to create a new context each time,
     * or clone one from a store.
     *
     * @return             valid, open FFI context object
     * @throws IOException if a native library couldn't be loaded
     */
    FFIContext<T> getFFIContext() throws IOException;
}
