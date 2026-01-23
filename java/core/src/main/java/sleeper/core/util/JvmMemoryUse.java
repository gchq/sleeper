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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * The amount of memory free and in use in the Java Virtual Machine. This is retrieved from methods on {@link Runtime}.
 * The names of the fields are inherited from {@link Runtime}, and their behaviour is unintuitive. Please see below.
 * <p>
 * Total memory is just the memory that is currently allocated by the operating system to the JVM. This includes memory
 * that is actually free space within Java. It does not include all the memory in the machine.
 * <p>
 * Free memory is space that has been allocated to the JVM but not yet used. It is not the amount of free memory in the
 * machine as a whole.
 * <p>
 * Max memory is the amount of space available to be allocated to the JVM by the operating system. This is not always
 * known. If there is no known max memory, it will be {@link Long#MAX_VALUE}.
 *
 * @param totalMemory the total amount of memory currently allocated for the JVM, in bytes
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

    /**
     * Gets a provider to read the current state of memory from the JVM runtime.
     *
     * @return the provider
     */
    public static Provider getProvider() {
        return new Provider() {
            @Override
            public JvmMemoryUse getMemory() {
                return from(Runtime.getRuntime());
            }

            @Override
            public long maxMemory() {
                return Runtime.getRuntime().maxMemory();
            }

            @Override
            @SuppressFBWarnings("DM_GC")
            public void gc() {
                System.gc();
            }
        };
    }

    public boolean isMaxMemoryKnown() {
        return maxMemory != Long.MAX_VALUE;
    }

    /**
     * Computes the amount of available memory in bytes, according to the maximum memory useable by the JVM. If the
     * maximum memory is not set, this will return a very large number.
     *
     * @return the amount of memory available in bytes
     */
    public long availableMemory() {
        return maxMemory - totalMemory + freeMemory;
    }

    /**
     * A provider to read the current state of memory. Can be used to fake the state of memory in tests.
     */
    public interface Provider {

        /**
         * Reads the current state of memory. Usually implemented with {@link Runtime}.
         *
         * @return the state of memory
         */
        JvmMemoryUse getMemory();

        /**
         * Reads the maximum amount of memory that can be allocated to the JVM. Usually implemented with
         * {@link Runtime#maxMemory()}.
         *
         * @return the maximum amount of memory
         */
        default long maxMemory() {
            return getMemory().maxMemory();
        }

        /**
         * Triggers garbage collection. Usually implemented with {@link System#gc()}.
         */
        default void gc() {
        }
    }

}
