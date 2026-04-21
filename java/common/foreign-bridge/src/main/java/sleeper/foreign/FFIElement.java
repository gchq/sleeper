/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.foreign;

import jnr.ffi.Struct;

/**
 * A Sleeper row key.
 *
 * <strong>THIS IS A C COMPATIBLE FFI STRUCT!</strong> If you updated this struct (field ordering, types, etc.),
 * you MUST update the corresponding Rust definition in rust/sleeper_df/src/objects.rs. The order and types of
 * the fields must match exactly.
 */
public class FFIElement extends Struct {
    public final Struct.Enum<FFIElementType> contained = new Struct.Enum<>(FFIElementType.class);
    public final Struct.StructRef<FFIElementData> item = new Struct.StructRef<>(FFIElementData.class);

    public FFIElement(jnr.ffi.Runtime runtime) {
        super(runtime);
    }
}
