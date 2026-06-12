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
 * distributed under the License on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sleeper.foreign.datafusion.extension;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import jnr.ffi.Struct;

/**
 * Contains extra data relating to a Sleeper compaction or query. Each extension type maybe specified once or multiple
 * times depending on its purpose.
 *
 * <strong>THIS IS A C COMPATIBLE FFI STRUCT!</strong> If you updated this struct (field ordering, types, etc.),
 * you MUST update the corresponding Rust definition in rust/sleeper_df/src/objects/extensions.rs. The order and types
 * of the fields must match exactly.
 */
@SuppressWarnings("checkstyle:membername")
@SuppressFBWarnings("PA_PUBLIC_MUTABLE_OBJECT_ATTRIBUTE")
public class FFIExtension extends Struct {
    /** The type of extension. */
    public final Struct.Enum<FFIExtensionVariant> variant = new Struct.Enum<>(FFIExtensionVariant.class);

    /** Information specific to this extension type. */
    public final FFIExtensionData data = inner(FFIExtensionData.class);

    public FFIExtension(jnr.ffi.Runtime runtime) {
        super(runtime);
    }

    /**
     * Set this extension as a SQL extension.
     *
     * @param extension the SQL extension
     */
    public void setSqlExtension(FFISQLExtension extension) {
        variant.set(FFIExtensionVariant.SQL);
        data.set(extension);
    }
}