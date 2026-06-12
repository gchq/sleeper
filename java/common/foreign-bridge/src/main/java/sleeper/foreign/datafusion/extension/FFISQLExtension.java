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

/**
 * Defines data required for the SQL in queries.
 *
 * <strong>THIS IS A C COMPATIBLE FFI STRUCT!</strong> If you updated this struct (field ordering, types, etc.),
 * you MUST update the corresponding Rust definition in rust/sleeper_df/src/objects/query_extensions.rs. The order and
 * types
 * of the fields must match exactly.
 */
@SuppressWarnings("checkstyle:membername")
@SuppressFBWarnings("PA_PUBLIC_MUTABLE_OBJECT_ATTRIBUTE")
public class FFISQLExtension extends Struct {
    /** The SQL query string to use for filtering a Sleeper query. */
    public final Struct.UTF8StringRef sql = new Struct.UTF8StringRef();

    public FFISQLExtension(jnr.ffi.Runtime runtime) {
        super(runtime);
    }

    public FFISQLExtension(jnr.ffi.Runtime runtime, java.lang.String sql) {
        this(runtime);
        this.sql.set(sql);
    }
}