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
package sleeper.query.datafusion;

import jnr.ffi.Struct;

/**
 * A C compatible class (struct) that is intended to be populated from the Rust
 * side of the FFI boundary. An instance of this class is created on the Java
 * side and then a pointer to it passed to Rust. The JNR-FFI library will ensure
 * that C layout and alignment rules are followed.
 *
 * <strong>THIS IS A C COMPATIBLE FFI STRUCT!</strong> If you updated this struct (field ordering, types, etc.),
 * you MUST update the corresponding Rust definition in rust/sleeper_df/src/objects.rs. The order and types of
 * the fields must match exactly. *
 */
public class FFIQueryResults extends Struct {

    /**
     * The pointer to where the Arrow FFI Array stream has been allocated
     * on the Rust side of the FFI boundary.
     */
    public final Struct.Pointer arrowArrayStreamPtr = new Struct.Pointer();

    /**
     * Create a holder for results data from the FFI side of the boundary.
     *
     * @param rt the JNR runtime
     */
    public FFIQueryResults(jnr.ffi.Runtime rt) {
        super(rt);
    }
}
