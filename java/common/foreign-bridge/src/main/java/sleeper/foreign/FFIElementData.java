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
import jnr.ffi.Union;

/**
 * Java mapping of union type for containing a Sleeper row key.
 * Unions in C use overlapping storage for their members. Only
 * one member may be "active" at a time.
 *
 * <strong>THIS IS A C COMPATIBLE FFI STRUCT!</strong> If you updated this struct (field ordering, types, etc.),
 * you MUST update the corresponding Rust definition in rust/sleeper_df/src/objects.rs. The order and types of
 * the fields must match exactly.
 */
public class FFIElementData extends Union {
    public final Struct.Signed32 int32=new Struct.Signed32();
    public final Struct.Signed64 int64=new Struct.Signed64();
    public final Struct.UTF8StringRef string=new Struct.UTF8StringRef();
    public final Struct. int32=new Struct.int32_t();



}
