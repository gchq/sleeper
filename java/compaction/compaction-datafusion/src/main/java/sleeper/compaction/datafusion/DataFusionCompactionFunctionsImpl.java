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

import sleeper.foreign.bridge.FFIBridge;

import java.io.IOException;

/**
 * Links to the DataFusion code for compactions in Rust. This is done in a separate class to delay static
 * initialization until it is needed.
 */
public class DataFusionCompactionFunctionsImpl {

    private DataFusionCompactionFunctionsImpl() {
    }

    /**
     * Creates the link to the DataFusion code in Rust.
     *
     * @return the Rust DataFusion implementation
     */
    public static DataFusionCompactionFunctions create() {
        try {
            return FFIBridge.createForeignInterface(DataFusionCompactionFunctions.class);
        } catch (IOException e) {
            throw new IllegalStateException("Could not load foreign interface", e);
        }
    }

}
