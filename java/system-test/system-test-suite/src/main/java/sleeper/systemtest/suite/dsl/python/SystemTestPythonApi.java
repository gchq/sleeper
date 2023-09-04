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

package sleeper.systemtest.suite.dsl.python;

import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.suite.fixtures.SystemTestClients;

import java.nio.file.Path;

public class SystemTestPythonApi {
    private final SleeperInstanceContext instance;
    private final SystemTestClients clients;
    private final Path pythonDir;
    private final Path outputDir;

    public SystemTestPythonApi(SleeperInstanceContext instance, SystemTestClients clients, Path pythonDir, Path tempDir) {
        this.instance = instance;
        this.clients = clients;
        this.pythonDir = pythonDir;
        this.outputDir = tempDir;
    }

    public SystemTestPythonIngest ingest() {
        return new SystemTestPythonIngest(instance, clients, pythonDir, outputDir);
    }

    public SystemTestPythonBulkImport bulkImport() {
        return new SystemTestPythonBulkImport(instance, clients, pythonDir);
    }
}
