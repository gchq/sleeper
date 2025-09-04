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

import jnr.ffi.Pointer;
import jnr.ffi.provider.jffi.ArrayMemoryIO;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FFIContextTest {

    @Test
    void shouldClose() {
        // Given
        FFIContext context = new FFIContext(new ForeignFunctions() {

            @Override
            public Pointer create_context() {
                return new ArrayMemoryIO(jnr.ffi.Runtime.getSystemRuntime(), 1);
            }

            @Override
            public void destroy_context(Pointer ctx) {

            }

        });

        // When - Then
        assertThat(context.isClosed()).isFalse();

        context.close();

        assertThat(context.isClosed()).isTrue();
    }
}
