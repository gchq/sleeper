/*
 * Copyright 2022-2024 Crown Copyright
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

import sleeper.systemtest.drivers.ingest.AwsInvokeIngestTasksDriver;
import sleeper.systemtest.drivers.python.PythonIngestDriver;
import sleeper.systemtest.drivers.util.AwsWaitForJobs;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.instance.SleeperInstanceContext;
import sleeper.systemtest.dsl.python.SystemTestPythonIngest;

import java.nio.file.Path;

public class SystemTestPythonApi {
    private final SleeperInstanceContext instance;
    private final SystemTestClients clients;
    private final Path pythonDir;

    public SystemTestPythonApi(SleeperInstanceContext instance, SystemTestClients clients, Path pythonDir) {
        this.instance = instance;
        this.clients = clients;
        this.pythonDir = pythonDir;
    }

    public SystemTestPythonIngest ingestByQueue() {
        return new SystemTestPythonIngest(
                new PythonIngestDriver(instance, pythonDir),
                new AwsInvokeIngestTasksDriver(instance, clients),
                AwsWaitForJobs.forIngest(instance, clients.getDynamoDB()));
    }

    public SystemTestPythonBulkImport bulkImport() {
        return new SystemTestPythonBulkImport(instance, clients, pythonDir);
    }

    public SystemTestPythonQuery query(Path outputDir) {
        return new SystemTestPythonQuery(instance, pythonDir, outputDir);
    }
}
