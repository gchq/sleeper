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
package sleeper.foreign.datafusion.extension;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import jnr.ffi.Struct;
import jnr.ffi.Union;

/**
 * Variant type for an extension of a specific type.
 * This is a union type, storage for all members overlaps!
 * <p>
 * <strong>THIS IS A C COMPATIBLE FFI STRUCT!</strong> If you updated this union (field ordering, types, etc.),
 * you MUST update the corresponding Rust definition in rust/sleeper_df/src/objects/extensions.rs. The order and types
 * of the fields must match exactly.
 */
@SuppressFBWarnings({"PA_PUBLIC_MUTABLE_OBJECT_ATTRIBUTE", "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR", "URF_UNREAD_FIELD"})
public class FFIExtensionData extends Union {
    /** SQL extension data. */
    public final Struct.StructRef<FFISQLExtension> sql = new Struct.StructRef<>(FFISQLExtension.class);
    /** Prevent GC. */
    private FFISQLExtension javaHolder;

    public FFIExtensionData(jnr.ffi.Runtime runtime) {
        super(runtime);
    }

    /**
     * Set the contents of this union to a SQL extension.
     *
     * @param extension the SQL extension
     */
    public void set(FFISQLExtension extension) {
        sql.set(extension);
        javaHolder = extension;
    }
}
