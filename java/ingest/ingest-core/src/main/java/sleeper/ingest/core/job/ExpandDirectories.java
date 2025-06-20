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
package sleeper.ingest.core.job;

import java.util.List;

/**
 * Takes paths specified in an ingest job and expands any directories, listing all files.
 */
public interface ExpandDirectories {

    /**
     * Expands the given paths. Reports on any paths that were not found.
     *
     * @param  files the paths
     * @return       the result
     */
    ExpandDirectoriesResult expandPaths(List<String> files);

}
