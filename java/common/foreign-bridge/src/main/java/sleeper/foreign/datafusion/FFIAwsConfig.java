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
package sleeper.foreign.datafusion;

import jnr.ffi.Struct;

/**
 * The AWS configuration to be supplied to DataFusion.
 *
 * <strong>THIS IS A C COMPATIBLE FFI STRUCT!</strong> If you updated this struct (field ordering, types, etc.),
 * you MUST update the corresponding Rust definition in rust/sleeper_df/src/objects/aws_config.rs. The order and types
 * of the fields must match exactly.
 */
@SuppressWarnings(value = "checkstyle:memberName")
public class FFIAwsConfig extends Struct {

    final Struct.UTF8StringRef region = new Struct.UTF8StringRef();
    final Struct.UTF8StringRef endpoint = new Struct.UTF8StringRef();
    final Struct.UTF8StringRef access_key = new Struct.UTF8StringRef();
    final Struct.UTF8StringRef secret_key = new Struct.UTF8StringRef();
    final Struct.UTF8StringRef session_token = new Struct.UTF8StringRef();
    final Struct.Boolean allow_http = new Struct.Boolean();

    public FFIAwsConfig(jnr.ffi.Runtime runtime) {
        super(runtime);
        region.set("");
        endpoint.set("");
        access_key.set("");
        secret_key.set("");
        session_token.set("");
    }
}
