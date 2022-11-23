/*
 * Copyright 2022 Crown Copyright
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
package sleeper.build.github.actions;

import java.util.List;

public class NotAllDependenciesDeclaredException extends RuntimeException {

    private final String chunkId;
    private final List<String> unconfiguredModuleRefs;

    public NotAllDependenciesDeclaredException(String chunkId, List<String> unconfiguredModuleRefs) {
        super("Maven dependencies not declared in on.push.paths for chunk \"" + chunkId + "\": " + unconfiguredModuleRefs);
        this.chunkId = chunkId;
        this.unconfiguredModuleRefs = unconfiguredModuleRefs;
    }

    public String getChunkId() {
        return chunkId;
    }

    public List<String> getUnconfiguredModuleRefs() {
        return unconfiguredModuleRefs;
    }
}
