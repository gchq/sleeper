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
package sleeper.compaction.datafusion;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import jnr.ffi.Struct;

/**
 * The compaction output data that the native code will populate.
 */
@SuppressWarnings(value = {"checkstyle:membername", "checkstyle:parametername"})
@SuppressFBWarnings(value = {"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class DataFusionCompactionResult extends Struct {
    public final Struct.size_t rows_read = new Struct.size_t();
    public final Struct.size_t rows_written = new Struct.size_t();

    public DataFusionCompactionResult(jnr.ffi.Runtime runtime) {
        super(runtime);
    }
}
