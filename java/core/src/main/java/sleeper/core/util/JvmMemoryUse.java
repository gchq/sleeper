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
package sleeper.core.util;

/**
 * The amount of memory free and in use in the Java Virtual Machine. This is retrieved from methods on {@link Runtime}.
 *
 * @param totalMemory the total amount of memory currently available for current and future objects, measured in bytes
 * @param freeMemory  an approximation to the total amount of memory currently available for future allocated objects,
 *                    measured in bytes
 * @param maxMemory   the maximum amount of memory that the virtual machine will attempt to use, measured in bytes
 */
public record JvmMemoryUse(long totalMemory, long freeMemory, long maxMemory) {

    /**
     * Reads the current state of memory from the runtime.
     *
     * @param  runtime the runtime
     * @return         the state of memory
     */
    public static JvmMemoryUse from(Runtime runtime) {
        return new JvmMemoryUse(runtime.totalMemory(), runtime.freeMemory(), runtime.maxMemory());
    }

    public static Provider getProvider() {
        return () -> from(Runtime.getRuntime());
    }

    /**
     * A provider to read the current state of memory. Can be used to fake the state of memory in tests.
     */
    public interface Provider {

        /**
         * Reads the current state of memory.
         *
         * @return the state of memory
         */
        JvmMemoryUse getMemory();
    }

}
