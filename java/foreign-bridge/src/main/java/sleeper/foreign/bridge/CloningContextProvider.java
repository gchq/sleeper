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
import java.util.Objects;

/**
 * An FFI context provider that clones the last context if available it is not closed.
 *
 * @param <T> the FFI interface class type
 */
public class CloningContextProvider<T extends ForeignFunctions> implements FFIContextProvider<T> {
    /**
     * FFI interface type.
     */
    private final Class<T> functionsClass;
    /**
     * Last context instance created. Access must be synchronized!
     */
    private FFIContext<T> lastContext = null;

    public CloningContextProvider(Class<T> functionsClass) {
        this.functionsClass = Objects.requireNonNull(functionsClass, "functionsClass");
    }

    @Override
    public synchronized FFIContext<T> getFFIContext() throws IOException {
        FFIContext<T> value;
        if (lastContext != null && !lastContext.isClosed()) {
            // Clone underlying context
            value = lastContext = new FFIContext<>(lastContext);
        } else {
            value = lastContext = new FFIContext<>(functionsClass);
        }
        return value;
    }
}
