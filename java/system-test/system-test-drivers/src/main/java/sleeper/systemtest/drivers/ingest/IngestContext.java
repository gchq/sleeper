/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.systemtest.drivers.ingest;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.ingest.IngestFactory;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

import java.nio.file.Path;

public class IngestContext {
    private final SleeperInstanceContext instance;

    public IngestContext(SleeperInstanceContext instance) {
        this.instance = instance;
    }

    public IngestFactory factory(Path tempDir) {
        return IngestFactory.builder()
                .objectFactory(ObjectFactory.noUserJars())
                .localDir(tempDir.toString())
                .stateStoreProvider(instance.getStateStoreProvider())
                .instanceProperties(instance.getInstanceProperties())
                .build();
    }
}
