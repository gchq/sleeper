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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import sleeper.clients.util.ShutdownWrapper;

public interface SleeperClientBufferAllocatorProvider {

    ShutdownWrapper<BufferAllocator> getBufferAllocator();

    /**
     * Creates a provider that will create a new root allocator for each Sleeper client, which will be closed when the
     * client is closed.
     *
     * @return the provider
     */
    static SleeperClientBufferAllocatorProvider createDefaultForEachClient() {
        return () -> {
            BufferAllocator allocator = new RootAllocator();
            return ShutdownWrapper.shutdown(allocator, allocator::close);
        };
    }

}
